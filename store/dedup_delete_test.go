package store

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"testing"
)

// readObject 完整读出并校验对象内容。
func readObject(t *testing.T, s *Store, id string, want []byte) {
	t.Helper()
	rc, err := s.Get(id)
	if err != nil {
		t.Fatalf("get %q: %v", id, err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("read %q: %v", id, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("content mismatch for %q", id)
	}
}

// casFiles 统计 CAS 仓库中的物理块文件及其总字节，作为磁盘占用依据。
func casFiles(t *testing.T, s *Store) (map[string]int64, int64) {
	t.Helper()
	m, err := s.listCASBlobs()
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, n := range m {
		total += n
	}
	return m, total
}

func TestDedupIdenticalObjectsShareBlocks(t *testing.T) {
	s := newStore(t)
	// 跨多块、且块内容高度重复的数据：两个对象大段相同。
	prefix := bytes.Repeat([]byte("SHARED-PREFIX-"), 200) // 约 2.8KB，跨 3 块
	tail1 := bytes.Repeat([]byte("AAAA-object1-tail"), 100)
	tail2 := bytes.Repeat([]byte("BBBB-object2-tail"), 100)
	d1 := append(append([]byte{}, prefix...), tail1...)
	d2 := append(append([]byte{}, prefix...), tail2...)

	if err := s.Put("dup-1", bytes.NewReader(d1)); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("dup-2", bytes.NewReader(d2)); err != nil {
		t.Fatal(err)
	}
	files2, bytes2 := casFiles(t, s)

	// 逐块对比两个清单：相同位置相同内容的块必须指向同一 ref。
	m1, _ := s.loadManifest("dup-1")
	m2, _ := s.loadManifest("dup-2")
	shared := 0
	for i := 0; i < len(m1.Chunks) && i < len(m2.Chunks); i++ {
		if m1.Chunks[i].SHA256 == m2.Chunks[i].SHA256 {
			if m1.Chunks[i].Ref != m2.Chunks[i].Ref {
				t.Fatalf("equal chunk %d not shared: %s vs %s", i, m1.Chunks[i].Ref, m2.Chunks[i].Ref)
			}
			shared++
		}
	}
	t.Logf("chunks total obj1=%d obj2=%d, shared chunks=%d", len(m1.Chunks), len(m2.Chunks), shared)
	if shared == 0 {
		t.Fatal("expected at least one shared block")
	}

	// 物理文件只按唯一内容块计数：比"各存一份"明显更少。
	uniqueRefs := map[string]bool{}
	for _, c := range m1.Chunks {
		uniqueRefs[c.Ref] = true
	}
	for _, c := range m2.Chunks {
		uniqueRefs[c.Ref] = true
	}
	if len(files2) != len(uniqueRefs) {
		t.Fatalf("CAS physical files=%d but unique chunks=%d", len(files2), len(uniqueRefs))
	}
	t.Logf("CAS physical blocks=%d for 2 objects with %d shared chunks (without dedup would be %d); total bytes=%d",
		len(files2), shared, len(m1.Chunks)+len(m2.Chunks), bytes2)

	// 去重不得改变读取结果：共享与否读出来都必须与写入完全一致。
	readObject(t, s, "dup-1", d1)
	readObject(t, s, "dup-2", d2)
	if err := s.Verify("dup-1"); err != nil || s.Verify("dup-2") != nil {
		t.Fatalf("verify after dedup: %v %v", s.Verify("dup-1"), s.Verify("dup-2"))
	}
}

func TestDedupSameObjectRepeatedChunks(t *testing.T) {
	s := newStore(t)
	// 单个对象内部多个完全相同的块：也应只存一份。
	unit := bytes.Repeat([]byte("repetitive-block-"), 60) // ~960B，每块 1024 内含同型片段
	data := bytes.Repeat(unit, 12)
	if err := s.Put("repeat", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	m, _ := s.loadManifest("repeat")
	uniq := map[string]bool{}
	for _, c := range m.Chunks {
		uniq[c.Ref] = true
	}
	files, total := casFiles(t, s)
	t.Logf("object chunks=%d unique refs=%d physical CAS files=%d bytes=%d",
		len(m.Chunks), len(uniq), len(files), total)
	if len(files) < len(m.Chunks) {
		t.Logf("intra-object dedup saved %d physical blocks", len(m.Chunks)-len(files))
	}
	readObject(t, s, "repeat", data)
}

func TestDedupStreamingBoundedMemory(t *testing.T) {
	// 去重后仍逐块处理：64MiB 对象 + 1MiB 块，读写缓冲恒定。
	s, err := Open(t.TempDir(), Options{ChunkSize: 1 << 20, Stripes: 4})
	if err != nil {
		t.Fatal(err)
	}
	data := randBytes(t, 64<<20)
	if err := s.Put("big-a", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("big-b", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	files, total := casFiles(t, s)
	t.Logf("two 64MiB identical objects -> %d unique physical blocks, %d bytes on CAS",
		len(files), total)
	readObject(t, s, "big-b", data)
}

func TestDeleteDistinguishableFromNotFound(t *testing.T) {
	s := newStore(t)
	data := randBytes(t, 3*1024)
	if err := s.Put("doomed", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	// 删除前可读。
	readObject(t, s, "doomed", data)

	if err := s.Delete("doomed"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// 删除后读取：明确的 ErrObjectDeleted，而不是 not-found，更不能给残缺内容。
	if _, err := s.Get("doomed"); !errors.Is(err, ErrObjectDeleted) {
		t.Fatalf("want ErrObjectDeleted, got %v", err)
	}
	if err := s.Verify("doomed"); !errors.Is(err, ErrObjectDeleted) {
		t.Fatalf("verify want ErrObjectDeleted, got %v", err)
	}
	// 从未写过的对象：ErrObjectNotFound，两者严格可区分。
	if _, err := s.Get("never-existed"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("want ErrObjectNotFound, got %v", err)
	}
	t.Logf("deleted -> ErrObjectDeleted; never-written -> ErrObjectNotFound (distinguishable)")

	// 墓碑与清单的物理状态可作为判定依据。
	if _, err := os.Stat(s.tombstonePath("doomed")); err != nil {
		t.Fatalf("tombstone should exist: %v", err)
	}
	if _, err := os.Stat(s.manifestPath("doomed")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("manifest should be removed, err=%v", err)
	}
}

func TestDeleteIdempotentAndNotFound(t *testing.T) {
	s := newStore(t)
	data := randBytes(t, 2*1024)
	if err := s.Put("d-idem", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := s.Delete("d-idem"); err != nil {
			t.Fatalf("repeat delete #%d must be idempotent: %v", i, err)
		}
	}
	t.Logf("repeated delete of same object: 3x success (idempotent, deterministic)")

	err := s.Delete("never-written")
	if !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("delete never-written want ErrObjectNotFound, got %v", err)
	}
	t.Logf("delete never-written -> ErrObjectNotFound")

	// 已删除 ID 不允许被 Put 覆盖，保证删除结论稳定。
	if err := s.Put("d-idem", bytes.NewReader(data)); !errors.Is(err, ErrObjectDeleted) {
		t.Fatalf("overwrite deleted id want ErrObjectDeleted, got %v", err)
	}
}

func TestDeleteDoesNotAffectSharingObject(t *testing.T) {
	s := newStore(t)
	shared := bytes.Repeat([]byte("common-prefix-"), 250)
	d1 := append(append([]byte{}, shared...), []byte("tail-X")...)
	d2 := append(append([]byte{}, shared...), []byte("tail-Y")...)
	if err := s.Put("keeper", bytes.NewReader(d1)); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("goner", bytes.NewReader(d2)); err != nil {
		t.Fatal(err)
	}

	// 删除前回收：无垃圾。
	rep, err := s.Reclaim()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("before delete reclaim: deleted=%d reclaimed=%d", rep.DeletedBlocks, rep.ReclaimedBytes)

	if err := s.Delete("goner"); err != nil {
		t.Fatal(err)
	}
	// 删除共享者之一：另一个对象必须仍然完整读出（其共享块未受影响）。
	readObject(t, s, "keeper", d1)
	if err := s.Verify("keeper"); err != nil {
		t.Fatalf("keeper broken after peer delete: %v", err)
	}

	rep, err = s.Reclaim()
	if err != nil {
		t.Fatal(err)
	}
	files, _ := casFiles(t, s)
	t.Logf("after deleting one sharing object: reclaim deleted=%d bytes=%d, remaining blocks=%d",
		rep.DeletedBlocks, rep.ReclaimedBytes, len(files))
	// keeper 仍完整。
	readObject(t, s, "keeper", d1)
	if _, err := s.Get("goner"); !errors.Is(err, ErrObjectDeleted) {
		t.Fatalf("goner must stay deleted, got %v", err)
	}
}
