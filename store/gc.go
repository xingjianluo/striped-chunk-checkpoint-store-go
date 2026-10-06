package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// computeCRC 覆盖计划 ID 与全部条目（含 ref 与长度），任何篡改都会失配。
func (p *ReclaimPlan) computeCRC() uint32 {
	h := crc32.NewIEEE()
	fmt.Fprintf(h, "id=%s;n=%d", p.ID, len(p.Entries))
	for _, e := range p.Entries {
		fmt.Fprintf(h, "|%s:%d", e.Ref, e.Length)
	}
	return h.Sum32()
}

func (p *ReclaimPlan) valid() error {
	if p.ID == "" {
		return errors.New("missing plan id")
	}
	var sum int64
	for _, e := range p.Entries {
		if len(e.Ref) != 64 {
			return fmt.Errorf("invalid ref %q", e.Ref)
		}
		if e.Length < 0 {
			return fmt.Errorf("negative length for %q", e.Ref)
		}
		sum += e.Length
	}
	if sum != p.TotalBytes {
		return fmt.Errorf("total bytes %d != entries sum %d", p.TotalBytes, sum)
	}
	if p.CRC != p.computeCRC() {
		return errors.New("plan crc mismatch")
	}
	return nil
}

func (s *Store) savePlan(p *ReclaimPlan) error {
	p.CRC = p.computeCRC()
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(s.gcDir(), 0o755); err != nil {
		return err
	}
	final := s.gcPlanPath()
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	return syncDir(s.gcDir())
}

// loadPlan 读取并严格校验回收计划；任何损坏/不可信都返回
// ErrReclaimPlanCorrupt，调用方必须拒绝据此删除数据。
func (s *Store) loadPlan() (*ReclaimPlan, error) {
	data, err := os.ReadFile(s.gcPlanPath())
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fs.ErrNotExist
		}
		return nil, err
	}
	var p ReclaimPlan
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrReclaimPlanCorrupt, err)
	}
	if err := p.valid(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrReclaimPlanCorrupt, err)
	}
	return &p, nil
}

func (s *Store) removePlan() error {
	if err := os.Remove(s.gcPlanPath()); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return syncDir(s.gcDir())
}

// listObjectIDs 列出全部已提交对象 ID（依据 objects 目录，不含墓碑）。
func (s *Store) listObjectIDs() ([]string, error) {
	dir := filepath.Join(s.root, "objects")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var ids []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".json") {
			continue
		}
		ids = append(ids, strings.TrimSuffix(name, ".json"))
	}
	return ids, nil
}

// Reclaim 执行一次空间回收（标记-清除），可安全重入与中断续跑。
//
// 安全保证：
//   - 全程持有生命周期写锁，与正在进行的读取/提交/删除互斥，
//     绝不会回收"正在写入或正在读取所依赖"的数据；
//   - 标记结果写入带 CRC 的计划文件，清除阶段逐项删除前再次核对
//     "当前是否仍无存活引用"，因此重复执行 / 中断续跑都不会误删
//     重新获得引用的数据，也不会重复计数；
//   - 计划文件损坏或不可信 -> ErrReclaimPlanCorrupt，明确拒绝删除；
//   - 已完成的项以 done 标记记录，崩溃重启后续跑只补做未完成项。
func (s *Store) Reclaim() (*ReclaimReport, error) {
	s.gcMu.Lock()
	defer s.gcMu.Unlock()
	s.life.Lock()
	defer s.life.Unlock()

	plan, err := s.loadPlan()
	resumed := false
	switch {
	case err == nil:
		resumed = true // 上次清除被中断，接着干
	case errors.Is(err, fs.ErrNotExist):
		plan, err = s.markReclaim()
		if err != nil {
			return nil, err
		}
		if err := s.savePlan(plan); err != nil {
			return nil, err
		}
	default:
		// 计划损坏：宁可不动，也绝不照损坏记录删数据。
		return nil, err
	}

	rep, err := s.sweepReclaim(plan)
	if err != nil {
		return nil, err
	}
	rep.Resumed = resumed
	return rep, nil
}

// markReclaim 汇总所有存活清单引用的内容块，求 CAS 仓库的差集，
// 得到"确定无引用"的候选块。任何存活清单无法解析都中止（fail-closed），
// 宁可少回收也不误删。
func (s *Store) markReclaim() (*ReclaimPlan, error) {
	ids, err := s.listObjectIDs()
	if err != nil {
		return nil, err
	}
	live := map[string]struct{}{}
	for _, id := range ids {
		m, err := s.loadManifest(id)
		if err != nil {
			return nil, fmt.Errorf("%w: cannot read live manifest %q during mark: %v",
				ErrReclaimPlanCorrupt, id, err)
		}
		for _, c := range m.Chunks {
			if c.Ref != "" {
				live[c.Ref] = struct{}{}
			}
		}
	}

	blobs, err := s.listCASBlobs()
	if err != nil {
		return nil, err
	}
	refs := make([]string, 0, len(blobs))
	for ref := range blobs {
		if _, used := live[ref]; !used {
			refs = append(refs, ref)
		}
	}
	sort.Strings(refs)
	plan := &ReclaimPlan{ID: strconv.FormatInt(time.Now().UnixNano(), 10)}
	for _, ref := range refs {
		plan.Entries = append(plan.Entries, gcPlanEntry{Ref: ref, Length: blobs[ref]})
		plan.TotalBytes += blobs[ref]
	}
	plan.CRC = plan.computeCRC()
	return plan, nil
}

// sweepReclaim 按计划逐项删除；删除前重新核对引用与长度，
// 已完成项跳过，保证续跑幂等、不重复计数。
func (s *Store) sweepReclaim(plan *ReclaimPlan) (*ReclaimReport, error) {
	if err := os.MkdirAll(s.gcDoneDir(plan.ID), 0o755); err != nil {
		return nil, err
	}
	// 清除阶段重建一次存活引用集合，避免计划生成后引用关系变化导致误删。
	live, err := s.currentLiveRefs()
	if err != nil {
		return nil, err
	}

	rep := &ReclaimReport{PlanID: plan.ID}
	for _, e := range plan.Entries {
		done := s.gcDonePath(plan.ID, e.Ref)
		if _, err := os.Stat(done); err == nil {
			continue // 上次已完成：不重复删除、不重复计数
		}

		// 测试 / 故障注入钩子：在删除前模拟中断（已完成项不受影响）。
		if s.onSweepBlock != nil {
			s.onSweepBlock(e.Ref)
		}

		if _, used := live[e.Ref]; used {
			rep.SkippedBlocks++ // 重新获得引用，绝不能删
			if err := s.writeDone(done); err != nil {
				return nil, err
			}
			continue
		}

		path := s.casPath(e.Ref)
		fi, err := os.Stat(path)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			// 块已不在（此前删过、计划过期）：记为完成，不计数。
			if err := s.writeDone(done); err != nil {
				return nil, err
			}
			continue
		case err != nil:
			return nil, err
		case fi.Size() != e.Length:
			// 物理块与计划记录不一致：记录不可信于此项，跳过并要求人工关注，
			// 不删除任何内容。
			rep.SkippedBlocks++
			if err := s.writeDone(done); err != nil {
				return nil, err
			}
			continue
		}

		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		if err := syncDir(filepath.Dir(path)); err != nil {
			return nil, err
		}
		if err := s.writeDone(done); err != nil {
			return nil, err
		}
		rep.DeletedBlocks++
		rep.ReclaimedBytes += e.Length
	}

	// 全部项处理完毕：移除完成标记目录与计划，下一次回收重新标记。
	if err := os.RemoveAll(s.gcDoneDir(plan.ID)); err != nil {
		return nil, err
	}
	if err := s.removePlan(); err != nil {
		return nil, err
	}
	return rep, nil
}

// currentLiveRefs 重新扫描全部存活清单，得到当前仍被引用的内容块集合。
func (s *Store) currentLiveRefs() (map[string]struct{}, error) {
	ids, err := s.listObjectIDs()
	if err != nil {
		return nil, err
	}
	live := map[string]struct{}{}
	for _, id := range ids {
		m, err := s.loadManifest(id)
		if err != nil {
			return nil, fmt.Errorf("%w: cannot read live manifest %q during sweep: %v",
				ErrReclaimPlanCorrupt, id, err)
		}
		for _, c := range m.Chunks {
			if c.Ref != "" {
				live[c.Ref] = struct{}{}
			}
		}
	}
	return live, nil
}

// writeDone 原子写入单项完成标记（先写临时文件再 rename）。
func (s *Store) writeDone(done string) error {
	if err := os.MkdirAll(filepath.Dir(done), 0o755); err != nil {
		return err
	}
	tmp := done + ".tmp"
	if err := os.WriteFile(tmp, []byte("done"), 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, done); err != nil {
		return err
	}
	return syncDir(filepath.Dir(done))
}
