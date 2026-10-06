package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
)

// diskUsage 统计目录下按 inode 去重后的实际磁盘占用（硬链接只计一次）。
func diskUsage(t *testing.T, root string) (bytes int64, files int) {
	t.Helper()
	seen := map[[2]uint64]bool{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		st, ok := fi.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("no stat_t for %s", path)
		}
		key := [2]uint64{uint64(st.Dev), uint64(st.Ino)}
		if !seen[key] {
			seen[key] = true
			bytes += fi.Size()
			files++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return bytes, files
}

// blobCount 统计内容寻址区中的 blob 数量。
func blobCount(t *testing.T, s *Store) int {
	t.Helper()
	n := 0
	filepath.WalkDir(filepath.Join(s.root, "data", "blobs"),
		func(path string, d fs.DirEntry, err error) error {
			if err == nil && d.Type().IsRegular() {
				n++
			}
			return nil
		})
	return n
}

func mustPut(t *testing.T, s *Store, id string, data []byte) {
	t.Helper()
	if err := s.Put(id, bytes.NewReader(data)); err != nil {
		t.Fatalf("put %q: %v", id, err)
	}
}

func mustRead(t *testing.T, s *Store, id string) []byte {
	t.Helper()
	rc, err := s.Get(id)
	if err != nil {
		t.Fatalf("get %q: %v", id, err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %q: %v", id, err)
	}
	return got
}

// TestDedupSharedAndOverlappingContent 验证相同内容只存一份，读取结果不变。
func TestDedupSharedAndOverlappingContent(t *testing.T) {
	s := newStore(t) // chunkSize=1024
	shared := randBytes(t, 8*1024)
	uniqC := randBytes(t, 4*1024)

	mustPut(t, s, "obj-a", shared)
	mustPut(t, s, "obj-b", shared) // 与 obj-a 完全相同
	// obj-c 与 obj-a 共享前 4 块，后 4 块独有。
	mustPut(t, s, "obj-c", append(append([]byte{}, shared[:4*1024]...), uniqC...))

	blobs := blobCount(t, s)
	// 期望：obj-a 8 块 + obj-c 独有 4 块 = 12 个 blob；
	// obj-b 的 8 块与 obj-c 的前 4 块全部去重命中。
	t.Logf("dedup: logical chunks=20 (8+8+4+... ), unique blobs on disk=%d", blobs)
	if blobs != 12 {
		t.Fatalf("want 12 unique blobs (8 shared + 4 unique to obj-c), got %d", blobs)
	}
	// 抽查共享 blob 的链接数：obj-a/obj-b/obj-c 共同引用的块 nlink=4。
	m, _ := s.loadManifest("obj-a")
	p := s.blobPath(m.Chunks[0].SHA256)
	if n, _ := blobLinkCount(p); n != 4 {
		t.Fatalf("shared blob link count=%d, want 4 (blob + 3 object links)", n)
	}
	t.Logf("shared chunk 0 (%s...): nlink=4 -> stored once, referenced by obj-a/b/c",
		m.Chunks[0].SHA256[:12])

	// 读取结果与写入完全一致，逐块校验通过。
	if got := mustRead(t, s, "obj-a"); !bytes.Equal(got, shared) {
		t.Fatal("obj-a read mismatch")
	}
	if got := mustRead(t, s, "obj-b"); !bytes.Equal(got, shared) {
		t.Fatal("obj-b read mismatch")
	}
	wantC := append(append([]byte{}, shared[:4*1024]...), uniqC...)
	if got := mustRead(t, s, "obj-c"); !bytes.Equal(got, wantC) {
		t.Fatal("obj-c read mismatch")
	}
	for _, id := range []string{"obj-a", "obj-b", "obj-c"} {
		if err := s.Verify(id); err != nil {
			t.Fatalf("verify %q: %v", id, err)
		}
	}
	t.Log("dedup read path: all objects read back identical to written content, verify ok")
}

// TestDedupTamperStillDetected 验证去重后篡改检测仍然有效。
func TestDedupTamperStillDetected(t *testing.T) {
	s := newStore(t)
	data := randBytes(t, 4*1024)
	mustPut(t, s, "obj-t1", data)
	mustPut(t, s, "obj-t2", data)

	m, _ := s.loadManifest("obj-t1")
	bp := s.blobPath(m.Chunks[1].SHA256)
	f, _ := os.OpenFile(bp, os.O_WRONLY, 0)
	f.WriteAt([]byte{0xFF, 0xFF}, 0) // 篡改共享 blob 内容（长度不变）
	f.Close()

	err := s.Verify("obj-t1")
	t.Logf("shared blob tampered -> verify obj-t1 err=%v", err)
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("want ErrChecksumMismatch, got %v", err)
	}
	rc, _ := s.Get("obj-t2")
	_, rerr := io.ReadAll(rc)
	rc.Close()
	t.Logf("streaming read of obj-t2 (same blob) -> err=%v", rerr)
	if !errors.Is(rerr, ErrChecksumMismatch) {
		t.Fatalf("want ErrChecksumMismatch from read, got %v", rerr)
	}
}

// TestDeleteStates 验证删除语义：已删除 / 从未写入可区分，重复删除幂等，
// 共享内容的其它对象不受影响。
func TestDeleteStates(t *testing.T) {
	s := newStore(t)
	shared := randBytes(t, 4*1024)
	mustPut(t, s, "obj-x", shared)
	mustPut(t, s, "obj-y", shared)

	if err := s.Delete("obj-x"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// 已删除 vs 从未写入：两种结论必须可区分。
	_, err := s.Get("obj-x")
	t.Logf("get deleted obj-x -> %v", err)
	if !errors.Is(err, ErrObjectDeleted) {
		t.Fatalf("want ErrObjectDeleted, got %v", err)
	}
	if verr := s.Verify("obj-x"); !errors.Is(verr, ErrObjectDeleted) {
		t.Fatalf("verify deleted: want ErrObjectDeleted, got %v", verr)
	}
	_, err = s.Get("obj-never")
	t.Logf("get never-written obj-never -> %v", err)
	if !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("want ErrObjectNotFound, got %v", err)
	}
	// 反复删除幂等、结果确定。
	if err := s.Delete("obj-x"); err != nil {
		t.Fatalf("re-delete must be idempotent nil, got %v", err)
	}
	if err := s.Delete("obj-x"); err != nil {
		t.Fatalf("re-delete again must be nil, got %v", err)
	}
	if err := s.Delete("obj-never"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("delete never-written: want ErrObjectNotFound, got %v", err)
	}
	t.Log("delete idempotent: deleted->nil, deleted->nil, never-written->ErrObjectNotFound")
	// 共享内容的 obj-y 不受影响（删除只移除了 obj-x 的链接）。
	if got := mustRead(t, s, "obj-y"); !bytes.Equal(got, shared) {
		t.Fatal("obj-y corrupted by deleting obj-x")
	}
	if err := s.Verify("obj-y"); err != nil {
		t.Fatalf("verify obj-y: %v", err)
	}
	t.Log("obj-y (sharing all chunks with deleted obj-x) still reads correctly")
}

// TestDeleteThenReput 验证删除后可重新写入同一对象。
func TestDeleteThenReput(t *testing.T) {
	s := newStore(t)
	v1 := randBytes(t, 3*1024)
	v2 := randBytes(t, 3*1024)
	mustPut(t, s, "obj-r", v1)
	if err := s.Delete("obj-r"); err != nil {
		t.Fatal(err)
	}
	mustPut(t, s, "obj-r", v2)
	if got := mustRead(t, s, "obj-r"); !bytes.Equal(got, v2) {
		t.Fatal("re-put after delete: content mismatch")
	}
	if _, err := s.loadTombstone("obj-r"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("tombstone must be cleared after recommit, got %v", err)
	}
	t.Log("re-put after delete: new content committed, tombstone cleared")
}

// TestGCReclaimsSpace 验证删除 + 回收后磁盘占用确实下降，
// 仍被引用的共享内容被保留，重复回收幂等不重复计数。
func TestGCReclaimsSpace(t *testing.T) {
	s := newStore(t)
	shared := randBytes(t, 4*1024)
	uniqA := randBytes(t, 4*1024)
	uniqB := randBytes(t, 4*1024)
	mustPut(t, s, "obj-ga", append(append([]byte{}, shared...), uniqA...))
	mustPut(t, s, "obj-gb", append(append([]byte{}, shared...), uniqB...))

	if err := s.Delete("obj-ga"); err != nil {
		t.Fatal(err)
	}
	before, _ := diskUsage(t, s.root)
	blobsBefore := blobCount(t, s)

	stats, err := s.Collect()
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	after, _ := diskUsage(t, s.root)
	blobsAfter := blobCount(t, s)
	t.Logf("gc: removed blobs=%d bytes=%d (resumed=%v); disk %d -> %d bytes; blobs %d -> %d",
		stats.BlobsRemoved, stats.BytesReclaimed, stats.Resumed,
		before, after, blobsBefore, blobsAfter)
	// obj-ga 独有的 4 块被回收，共享的 4 块因 obj-gb 仍在引用而保留。
	if stats.BlobsRemoved != 4 || stats.BytesReclaimed != 4*1024 {
		t.Fatalf("want 4 blobs / 4096 bytes reclaimed, got %d / %d",
			stats.BlobsRemoved, stats.BytesReclaimed)
	}
	if after >= before {
		t.Fatalf("disk usage did not drop: before=%d after=%d", before, after)
	}
	if blobsAfter != 8 {
		t.Fatalf("shared 4 + obj-gb unique 4 = 8 blobs must remain, got %d", blobsAfter)
	}
	t.Log("retention basis: shared blobs have nlink>=2 (referenced by obj-gb) -> kept")

	// 未删除对象完整可读。
	if err := s.Verify("obj-gb"); err != nil {
		t.Fatalf("verify obj-gb after gc: %v", err)
	}
	want := append(append([]byte{}, shared...), uniqB...)
	if got := mustRead(t, s, "obj-gb"); !bytes.Equal(got, want) {
		t.Fatal("obj-gb content mismatch after gc")
	}

	// 重复执行：无垃圾可回收，计数为 0（不重复计数、不误删）。
	stats2, err := s.Collect()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("second collect: %+v", stats2)
	if stats2.BlobsRemoved != 0 || stats2.BytesReclaimed != 0 || stats2.DirsRemoved != 0 {
		t.Fatalf("second collect must be a no-op, got %+v", stats2)
	}
	if err := s.Verify("obj-gb"); err != nil {
		t.Fatalf("verify obj-gb after second gc: %v", err)
	}
}

// TestGCInterruptResume 验证回收中断后重新触发能接着干完。
func TestGCInterruptResume(t *testing.T) {
	s := newStore(t)
	victim := randBytes(t, 6*1024) // 6 个独有块
	live := randBytes(t, 4*1024)
	mustPut(t, s, "obj-victim", victim)
	mustPut(t, s, "obj-live", live)
	if err := s.Delete("obj-victim"); err != nil {
		t.Fatal(err)
	}

	// 注入中断：回收掉 2 个 blob 后模拟崩溃。
	s.applyHook = func(kind string, done int) error {
		if kind == "blob" && done >= 2 {
			return errors.New("simulated crash during gc")
		}
		return nil
	}
	stats1, err := s.Collect()
	t.Logf("interrupted collect: stats=%+v err=%v", stats1, err)
	if err == nil {
		t.Fatal("expected injected crash")
	}
	if stats1.BlobsRemoved != 2 {
		t.Fatalf("want 2 blobs removed before crash, got %d", stats1.BlobsRemoved)
	}
	if _, err := os.Stat(s.gcPlanPath()); err != nil {
		t.Fatalf("gc plan must survive interruption: %v", err)
	}
	t.Log("gc plan persisted at gc/plan.json -> resumable")

	// 重新触发：接续上次计划，回收剩余 4 个 blob。
	s.applyHook = nil
	stats2, err := s.Collect()
	if err != nil {
		t.Fatalf("resumed collect: %v", err)
	}
	t.Logf("resumed collect: %+v", stats2)
	if !stats2.Resumed {
		t.Fatal("second collect must resume the interrupted plan")
	}
	if stats2.BlobsRemoved != 4 {
		t.Fatalf("want remaining 4 blobs removed, got %d", stats2.BlobsRemoved)
	}
	if n := blobCount(t, s); n != 4 {
		t.Fatalf("only obj-live's 4 blobs may remain, got %d", n)
	}
	// 计划完成后被清除；再跑一轮为空操作。
	if _, err := os.Stat(s.gcPlanPath()); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("plan must be removed after completion: %v", err)
	}
	stats3, _ := s.Collect()
	if stats3.BlobsRemoved != 0 || stats3.BytesReclaimed != 0 {
		t.Fatalf("third collect must be no-op, got %+v", stats3)
	}
	if err := s.Verify("obj-live"); err != nil {
		t.Fatalf("live object after interrupted+resumed gc: %v", err)
	}
	t.Log("interrupt/resume: 2 + 4 blobs reclaimed across two runs, live object intact")
}

// TestGCRejectsCorruptPlan 验证损坏或不可信的回收记录被明确拒绝。
func TestGCRejectsCorruptPlan(t *testing.T) {
	s := newStore(t)
	mustPut(t, s, "obj-safe", randBytes(t, 4*1024))
	mustPut(t, s, "obj-gone", randBytes(t, 2*1024))
	if err := s.Delete("obj-gone"); err != nil {
		t.Fatal(err)
	}
	blobsBefore := blobCount(t, s)

	// 1) 内容损坏的计划（CRC 不符）。
	os.WriteFile(s.gcPlanPath(), []byte(`{"version":1,"created_at":1,"dirs":[],"blobs":["`+
		fmt.Sprintf("%064x", 1)+`"],"crc":123}`), 0o644)
	_, err := s.Collect()
	t.Logf("corrupt-crc plan -> collect err=%v", err)
	if !errors.Is(err, ErrGCPlanCorrupt) {
		t.Fatalf("want ErrGCPlanCorrupt, got %v", err)
	}
	if n := blobCount(t, s); n != blobsBefore {
		t.Fatalf("corrupt plan must not delete anything: blobs %d -> %d", blobsBefore, n)
	}

	// 2) 条目不可信的计划（路径穿越），CRC 正确也不能通过。
	raw := []byte(`{"version":1,"created_at":1,"dirs":["../../etc"],"blobs":["../../objects/obj-safe.json"],"crc":0}`)
	var forged GCPlan
	if jerr := json.Unmarshal(raw, &forged); jerr != nil {
		t.Fatal(jerr)
	}
	forged.CRC = forged.computeCRC() // 攻击者连 CRC 都伪造好了
	if serr := s.savePlan(&forged); serr != nil {
		t.Fatal(serr)
	}
	_, err = s.Collect()
	t.Logf("untrusted-entries plan (forged crc) -> collect err=%v", err)
	if !errors.Is(err, ErrGCPlanCorrupt) {
		t.Fatalf("want ErrGCPlanCorrupt for untrusted entries, got %v", err)
	}

	// 3) 拒绝之后：数据完好，重新触发能正常制定新计划并完成回收。
	if err := s.Verify("obj-safe"); err != nil {
		t.Fatalf("obj-safe must survive rejected plans: %v", err)
	}
	stats, err := s.Collect()
	if err != nil {
		t.Fatalf("collect after rejected plans: %v", err)
	}
	t.Logf("fresh collect after rejections: %+v", stats)
	if stats.BlobsRemoved != 2 {
		t.Fatalf("want obj-gone's 2 blobs reclaimed, got %d", stats.BlobsRemoved)
	}
	if err := s.Verify("obj-safe"); err != nil {
		t.Fatal(err)
	}
}

// TestGCConcurrentReadWrite 验证回收与并发读写同时进行时的安全性。
func TestGCConcurrentReadWrite(t *testing.T) {
	s := newStore(t)
	// 常驻对象：全程可读，内容必须始终一致。
	live := map[string][]byte{}
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("live-%d", i)
		data := randBytes(t, 6*1024)
		mustPut(t, s, id, data)
		live[id] = data
	}
	// 待删除对象：制造可回收垃圾。
	for i := 0; i < 4; i++ {
		mustPut(t, s, fmt.Sprintf("doomed-%d", i), randBytes(t, 4*1024))
	}
	for i := 0; i < 4; i++ {
		if err := s.Delete(fmt.Sprintf("doomed-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	var readers, workers sync.WaitGroup
	stop := make(chan struct{})
	errCh := make(chan error, 64)

	// 读者：反复读取常驻对象并校验内容。
	for i := 0; i < 3; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for id, want := range live {
					rc, err := s.Get(id)
					if err != nil {
						errCh <- fmt.Errorf("get %q during gc: %w", id, err)
						return
					}
					got, err := io.ReadAll(rc)
					rc.Close()
					if err != nil {
						errCh <- fmt.Errorf("read %q during gc: %w", id, err)
						return
					}
					if !bytes.Equal(got, want) {
						errCh <- fmt.Errorf("content of %q changed during gc", id)
						return
					}
				}
			}
		}()
	}
	// 写者：持续写入新对象（与回收并发）。
	for i := 0; i < 2; i++ {
		workers.Add(1)
		go func(base int) {
			defer workers.Done()
			for j := 0; j < 15; j++ {
				id := fmt.Sprintf("w%d-%d", base, j)
				if err := s.Put(id, bytes.NewReader(randBytes(t, 3*1024))); err != nil {
					errCh <- fmt.Errorf("put %q during gc: %w", id, err)
					return
				}
			}
		}(i)
	}
	// 回收者：反复触发回收。
	var lastStats *GCStats
	workers.Add(1)
	go func() {
		defer workers.Done()
		for j := 0; j < 10; j++ {
			st, err := s.Collect()
			if err != nil {
				errCh <- fmt.Errorf("collect: %w", err)
				return
			}
			lastStats = st
		}
	}()

	// 写者与回收者完成后停止读者。
	workers.Wait()
	close(stop)
	readers.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
	t.Logf("concurrent gc+read+write finished; last collect stats=%+v", lastStats)

	// 终态：常驻对象全部完整，被删对象状态明确，新写入对象可读。
	for id := range live {
		if err := s.Verify(id); err != nil {
			t.Fatalf("verify %q: %v", id, err)
		}
	}
	for i := 0; i < 4; i++ {
		if _, err := s.Get(fmt.Sprintf("doomed-%d", i)); !errors.Is(err, ErrObjectDeleted) {
			t.Fatalf("doomed-%d: want ErrObjectDeleted, got %v", i, err)
		}
	}
	for base := 0; base < 2; base++ {
		for j := 0; j < 15; j++ {
			if err := s.Verify(fmt.Sprintf("w%d-%d", base, j)); err != nil {
				t.Fatalf("verify w%d-%d: %v", base, j, err)
			}
		}
	}
	t.Log("all live objects verified; deleted objects report ErrObjectDeleted; written-during-gc objects verified")
}

// TestLegacyObjectCompat 验证去重前的老格式对象（普通块文件、无 blob）
// 仍可正常读取、校验与删除，无需迁移。
func TestLegacyObjectCompat(t *testing.T) {
	s := newStore(t)
	data := randBytes(t, 3*1024+7)

	// 手工构造老格式对象：块是普通文件而非硬链接。
	chunks := make([]ChunkMeta, 0)
	for i := 0; i < 4; i++ {
		lo, hi := i*1024, (i+1)*1024
		if hi > len(data) {
			hi = len(data)
		}
		part := data[lo:hi]
		p := s.chunkPath("legacy", i)
		os.MkdirAll(filepath.Dir(p), 0o755)
		if err := os.WriteFile(p, part, 0o644); err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, ChunkMeta{
			Index: i, Stripe: s.stripeFor(i), Length: int64(len(part)), SHA256: sha256Hex(part),
		})
	}
	m := &Manifest{ObjectID: "legacy", ChunkSize: 1024, TotalSize: int64(len(data)),
		ChunkCount: len(chunks), Chunks: chunks, Digest: sha256Hex(data)}
	if err := s.commitManifest(m); err != nil {
		t.Fatal(err)
	}

	if got := mustRead(t, s, "legacy"); !bytes.Equal(got, data) {
		t.Fatal("legacy object read mismatch")
	}
	if err := s.Verify("legacy"); err != nil {
		t.Fatalf("verify legacy: %v", err)
	}
	t.Log("legacy object (plain chunk files, no blobs): read + verify ok, no migration needed")

	if err := s.Delete("legacy"); err != nil {
		t.Fatalf("delete legacy: %v", err)
	}
	if _, err := s.Get("legacy"); !errors.Is(err, ErrObjectDeleted) {
		t.Fatalf("deleted legacy: want ErrObjectDeleted, got %v", err)
	}
	before, _ := diskUsage(t, s.root)
	if _, err := s.Collect(); err != nil {
		t.Fatal(err)
	}
	after, _ := diskUsage(t, s.root)
	t.Logf("legacy delete+gc: disk %d -> %d bytes (legacy chunks removed at delete time)", before, after)
}
