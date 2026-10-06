package store

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// gcPlanVersion 为回收计划格式版本。
const gcPlanVersion = 1

// computeCRC 对回收计划关键字段计算校验值。
func (p *GCPlan) computeCRC() uint32 {
	h := crc32.NewIEEE()
	fmt.Fprintf(h, "%d|%d|%s|%s", p.Version, p.CreatedAt,
		strings.Join(p.Dirs, ","), strings.Join(p.Blobs, ","))
	return h.Sum32()
}

// Collect 回收不再被任何对象引用的空间，返回实际回收结果。
//
// 流程：制定回收计划并落盘（gc/plan.json，含 CRC）→ 按计划逐项执行
// → 删除计划。任何时刻中断，再次调用都会接续未完成的计划；
// 每个删除操作本身幂等，重复执行不会误删也不会重复计数。
//
// 安全规则：
//   - blob 仅当硬链接数 == 1（无任何对象块引用）时才回收，
//     且删除前在 gcMu 写锁内重新校验，正在写入/读取依赖的数据不会被回收；
//   - 对象块目录仅当对象有有效墓碑、且无清单、无写入检查点时才清理；
//   - 计划损坏或条目不可信（非法哈希 / 非法对象 ID）→ ErrGCPlanCorrupt，
//     明确拒绝，绝不照着它删数据。
func (s *Store) Collect() (*GCStats, error) {
	plan, resumed, err := s.loadOrMakePlan()
	if err != nil {
		return nil, err
	}
	stats := &GCStats{Resumed: resumed}
	if err := s.applyPlan(plan, stats); err != nil {
		return stats, err
	}
	// 全部完成：移除计划，下一轮重新制定。
	if err := os.Remove(s.gcPlanPath()); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return stats, err
	}
	return stats, nil
}

// loadOrMakePlan 读取上次未完成的计划；没有则重新制定并落盘。
// 计划损坏：丢弃该计划并返回 ErrGCPlanCorrupt（拒绝按不可信记录删除），
// 下次调用将重新制定。
func (s *Store) loadOrMakePlan() (*GCPlan, bool, error) {
	data, err := os.ReadFile(s.gcPlanPath())
	if err == nil {
		plan, perr := parseGCPlan(data)
		if perr != nil {
			// 不可信的回收记录：拒绝执行，移除以免反复误用。
			os.Remove(s.gcPlanPath())
			return nil, false, perr
		}
		return plan, true, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, false, err
	}
	plan, err := s.makePlan()
	if err != nil {
		return nil, false, err
	}
	if len(plan.Dirs) == 0 && len(plan.Blobs) == 0 {
		return plan, false, nil // 无垃圾：不落盘，直接返回空计划
	}
	if err := s.savePlan(plan); err != nil {
		return nil, false, err
	}
	return plan, false, nil
}

// parseGCPlan 解析并校验回收计划；任何不可信之处都拒绝。
func parseGCPlan(data []byte) (*GCPlan, error) {
	var p GCPlan
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("%w: unreadable plan", ErrGCPlanCorrupt)
	}
	if p.Version != gcPlanVersion || p.CreatedAt < 0 {
		return nil, fmt.Errorf("%w: invalid header", ErrGCPlanCorrupt)
	}
	if p.CRC != p.computeCRC() {
		return nil, fmt.Errorf("%w: crc mismatch", ErrGCPlanCorrupt)
	}
	for _, id := range p.Dirs {
		if !validObjectID(id) {
			return nil, fmt.Errorf("%w: untrusted dir entry %q", ErrGCPlanCorrupt, id)
		}
	}
	for _, b := range p.Blobs {
		if !validBlobHash(b) {
			return nil, fmt.Errorf("%w: untrusted blob entry %q", ErrGCPlanCorrupt, b)
		}
	}
	return &p, nil
}

// validObjectID 拒绝可能逃逸目录的对象 ID。
func validObjectID(id string) bool {
	return id != "" && id != "." && id != ".." &&
		!strings.ContainsAny(id, "/\\") && !strings.Contains(id, "\x00")
}

// validBlobHash 只接受 64 位十六进制 SHA-256，防止路径穿越。
func validBlobHash(h string) bool {
	if len(h) != 64 {
		return false
	}
	_, err := hex.DecodeString(h)
	return err == nil
}

// makePlan 扫描存储，找出可回收的目录与 blob。
func (s *Store) makePlan() (*GCPlan, error) {
	plan := &GCPlan{Version: gcPlanVersion, CreatedAt: time.Now().Unix()}

	// 1) 被中断删除的残留：有有效墓碑的对象，其块目录可清理。
	entries, err := os.ReadDir(filepath.Join(s.root, "objects"))
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		id, ok := strings.CutSuffix(e.Name(), ".tombstone")
		if !ok {
			continue
		}
		if _, err := s.loadTombstone(id); err != nil {
			continue // 墓碑损坏：不纳入计划，由 Delete/Get 显式报错
		}
		plan.Dirs = append(plan.Dirs, id)
	}

	// 2) 无引用的 blob：硬链接数 == 1 表示没有任何对象块指向它。
	blobsRoot := filepath.Join(s.root, "data", "blobs")
	err = filepath.WalkDir(blobsRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		hash := d.Name()
		if !validBlobHash(hash) {
			return nil // 非 blob 文件（如临时文件残留），不纳入计划
		}
		n, err := blobLinkCount(path)
		if err != nil {
			return nil // 统计失败时保守跳过
		}
		if n == 1 {
			plan.Blobs = append(plan.Blobs, hash)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return plan, nil
}

// savePlan 原子写入回收计划：临时文件 + rename + 目录同步。
func (s *Store) savePlan(p *GCPlan) error {
	p.CRC = p.computeCRC()
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	path := s.gcPlanPath()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// applyPlan 逐项执行回收计划。每项删除前重新校验引用状态，
// 已不存在的条目跳过（幂等），因此中断后重放同一计划即可续跑。
func (s *Store) applyPlan(plan *GCPlan, stats *GCStats) error {
	for _, id := range plan.Dirs {
		if err := s.collectDir(id, stats); err != nil {
			return err
		}
		if s.applyHook != nil {
			if err := s.applyHook("dir", stats.DirsRemoved); err != nil {
				return err
			}
		}
	}
	for _, hash := range plan.Blobs {
		if err := s.collectBlob(hash, stats); err != nil {
			return err
		}
		if s.applyHook != nil {
			if err := s.applyHook("blob", stats.BlobsRemoved); err != nil {
				return err
			}
		}
	}
	return nil
}

// collectDir 清理被删除对象的残留块目录。执行时在对象锁内重新校验：
// 墓碑仍有效、清单仍不存在、无写入中的检查点，三者齐备才删除。
func (s *Store) collectDir(id string, stats *GCStats) error {
	unlock := s.lockObject(id)
	defer unlock()
	if _, err := s.loadTombstone(id); err != nil {
		return nil // 墓碑消失（对象被重新写入）或损坏：跳过
	}
	if _, err := s.loadManifest(id); err == nil {
		return nil // 对象已重新提交：不得清理
	}
	if _, err := s.loadCheckpoint(id); err == nil {
		return nil // 有写入中的检查点：不得清理
	}
	removed := false
	for i := 0; i < s.stripes; i++ {
		dir := filepath.Join(s.root, "data", fmt.Sprintf("stripe-%d", i), id)
		if _, err := os.Stat(dir); err == nil {
			removed = true
		}
		if err := os.RemoveAll(dir); err != nil {
			return err
		}
	}
	if removed {
		stats.DirsRemoved++
	}
	return nil
}

// collectBlob 回收无引用 blob。在 gcMu 写锁内重新校验链接数，
// 与写入方的"建 blob + 建链接"（持读锁）互斥，杜绝误删。
func (s *Store) collectBlob(hash string, stats *GCStats) error {
	path := s.blobPath(hash)
	s.gcMu.Lock()
	defer s.gcMu.Unlock()
	n, err := blobLinkCount(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil // 已被上一轮删除：幂等跳过，不重复计数
	}
	if err != nil {
		return err
	}
	if n > 1 {
		return nil // 计划制定后又被引用：不得删除
	}
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	stats.BlobsRemoved++
	stats.BytesReclaimed += fi.Size()
	return syncDir(filepath.Dir(path))
}
