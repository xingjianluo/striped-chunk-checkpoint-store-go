package store

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func TestReclaimFreesOnlyUnreferencedSpace(t *testing.T) {
	s := newStore(t)
	// keeper 独有 K 块；goner 独有 G 块；两者共享 S 块。
	shared := bytes.Repeat([]byte("SHARED"), 120) // ~720B 同块
	kTail := bytes.Repeat([]byte("KKKK"), 200)
	gTail := bytes.Repeat([]byte("GGGG"), 200)
	dKeep := append(append([]byte{}, shared...), kTail...)
	dGone := append(append([]byte{}, shared...), gTail...)
	if err := s.Put("keeper", bytes.NewReader(dKeep)); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("goner", bytes.NewReader(dGone)); err != nil {
		t.Fatal(err)
	}

	_, before := casFiles(t, s)
	if rep, err := s.Reclaim(); err != nil {
		t.Fatal(err)
	} else {
		t.Logf("reclaim with no garbage: deleted=%d bytes=%d (expect 0)",
			rep.DeletedBlocks, rep.ReclaimedBytes)
		if rep.DeletedBlocks != 0 || rep.ReclaimedBytes != 0 {
			t.Fatal("nothing should be reclaimed while all objects live")
		}
	}

	if err := s.Delete("goner"); err != nil {
		t.Fatal(err)
	}
	rep, err := s.Reclaim()
	if err != nil {
		t.Fatal(err)
	}
	_, after := casFiles(t, s)
	t.Logf("reclaim after delete: deleted_blocks=%d reclaimed_bytes=%d; CAS bytes %d -> %d (freed %d)",
		rep.DeletedBlocks, rep.ReclaimedBytes, before, after, before-after)

	if rep.ReclaimedBytes <= 0 {
		t.Fatal("expected positive reclaimed bytes")
	}
	if before-after != rep.ReclaimedBytes {
		t.Fatalf("physical decrease %d != report %d", before-after, rep.ReclaimedBytes)
	}

	// 判定保留依据：keeper 清单引用的每个 ref 必须仍在磁盘上。
	m, _ := s.loadManifest("keeper")
	for _, c := range m.Chunks {
		if !s.casBlobExists(c.Ref, c.Length) {
			t.Fatalf("referenced block %s of keeper was wrongly reclaimed", c.Ref[:12])
		}
	}
	t.Logf("retention basis: all %d refs of keeper present; shared content preserved", len(m.Chunks))
	readObject(t, s, "keeper", dKeep)

	// 重复回收不再计数。
	rep2, err := s.Reclaim()
	if err != nil {
		t.Fatal(err)
	}
	if rep2.DeletedBlocks != 0 || rep2.ReclaimedBytes != 0 {
		t.Fatalf("second reclaim must not double count, got %+v", rep2)
	}
	t.Logf("repeat reclaim: deleted=%d bytes=%d (idempotent, no double count)",
		rep2.DeletedBlocks, rep2.ReclaimedBytes)
}

func TestReclaimInterruptedResumes(t *testing.T) {
	s := newStore(t)
	// 创建 3 个对象后全部删除，产生多个待回收块。
	var datas [][]byte
	for i := 0; i < 3; i++ {
		d := append([]byte("obj"), bytes.Repeat([]byte{byte('A' + i)}, 1500)...)
		datas = append(datas, d)
		if err := s.Put("tmp-"+string(rune('a'+i)), bytes.NewReader(d)); err != nil {
			t.Fatal(err)
		}
	}
	ids := []string{"tmp-a", "tmp-b", "tmp-c"}
	for _, id := range ids {
		if err := s.Delete(id); err != nil {
			t.Fatal(err)
		}
	}
	_, before := casFiles(t, s)

	// 故障注入：清除到第 2 个块时中断（panic 模拟进程崩溃，done 标记未写）。
	var seen int
	s.onSweepBlock = func(ref string) {
		seen++
		if seen == 2 {
			s.onSweepBlock = nil
			panic("simulated crash mid-sweep")
		}
	}
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected injected panic")
			} else {
				t.Logf("sweep interrupted after processing %d blocks: %v", seen-1, r)
			}
		}()
		_, _ = s.Reclaim()
	}()

	// 中断后计划与完成标记必须仍在磁盘（可续跑依据）。
	plan, err := s.loadPlan()
	if err != nil {
		t.Fatalf("plan must survive interruption: %v", err)
	}
	doneEntries := countDoneEntries(t, s, plan.ID)
	t.Logf("interrupted plan id=%s candidates=%d, done markers=%d (resume basis)",
		plan.ID, len(plan.Entries), doneEntries)
	if doneEntries != 1 {
		t.Fatalf("want exactly 1 durable done marker before crash, got %d", doneEntries)
	}

	// 重新触发：接着干完，只计数未完成部分，不重复删除已完成项。
	rep, err := s.Reclaim()
	if err != nil {
		t.Fatalf("resumed reclaim: %v", err)
	}
	if !rep.Resumed {
		t.Fatal("resumed run must report Resumed=true")
	}
	_, after := casFiles(t, s)
	t.Logf("resumed reclaim: deleted=%d bytes=%d (skipped done markers), CAS %d -> %d",
		rep.DeletedBlocks, rep.ReclaimedBytes, before, after)

	if rep.DeletedBlocks != len(plan.Entries)-doneEntries {
		t.Fatalf("resume deleted %d, want remaining %d",
			rep.DeletedBlocks, len(plan.Entries)-doneEntries)
	}
	if after != 0 {
		t.Fatalf("all objects deleted, CAS should be empty, got %d bytes", after)
	}
	// 计划完成后应被清理，下一次重新标记。
	if _, err := os.Stat(s.gcPlanPath()); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("plan should be removed after completion, stat err=%v", err)
	}
}

func TestReclaimRejectsCorruptPlan(t *testing.T) {
	s := newStore(t)
	d := bytes.Repeat([]byte("x"), 3000)
	if err := s.Put("vic", bytes.NewReader(d)); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("vic"); err != nil {
		t.Fatal(err)
	}

	// 先正常标记，生成一个可信计划，然后篡改其内容。
	plan, err := s.markReclaim()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.savePlan(plan); err != nil {
		t.Fatal(err)
	}
	if len(plan.Entries) == 0 {
		t.Fatal("expected candidates")
	}
	raw, err := os.ReadFile(s.gcPlanPath())
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xFF
	if err := os.WriteFile(s.gcPlanPath(), raw, 0o644); err != nil {
		t.Fatal(err)
	}

	_, before := casFiles(t, s)
	_, err = s.Reclaim()
	if !errors.Is(err, ErrReclaimPlanCorrupt) {
		t.Fatalf("corrupt plan must be rejected, got %v", err)
	}
	_, after := casFiles(t, s)
	t.Logf("corrupt plan rejected with ErrReclaimPlanCorrupt; CAS bytes unchanged %d -> %d",
		before, after)
	if before != after {
		t.Fatal("no data may be deleted based on an untrusted plan")
	}
}

// countDoneEntries 统计续跑标记目录中已完成的 64 位哈希块标记数。
func countDoneEntries(t *testing.T, s *Store, planID string) int {
	t.Helper()
	n := 0
	root := s.gcDoneDir(planID)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && len(d.Name()) == 64 {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}
