package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// writeV1StoreOnDisk 手工构造一个"升级前"的 v1 存储：
// 块放在对象私有条带目录，清单无 version、无 ref 字段。
func writeV1StoreOnDisk(t *testing.T, root, id string, data []byte, chunkSize, stripes int) {
	t.Helper()
	s := &Store{root: root, chunkSize: chunkSize, stripes: stripes}
	if err := os.MkdirAll(filepath.Join(root, "objects"), 0o755); err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	m := &Manifest{ObjectID: id, ChunkSize: chunkSize}
	for off, idx := 0, 0; off < len(data); off, idx = off+chunkSize, idx+1 {
		end := off + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunk := data[off:end]
		p := s.chunkPath(id, idx)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, chunk, 0o644); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(chunk)
		m.Chunks = append(m.Chunks, ChunkMeta{
			Index: idx, Stripe: idx % stripes,
			Length: int64(len(chunk)), SHA256: hex.EncodeToString(sum[:]),
		}) // 注意：刻意不设置 Ref，模拟老版本序列化结果
		h.Write(chunk)
	}
	m.TotalSize = int64(len(data))
	m.ChunkCount = len(m.Chunks)
	m.Digest = hex.EncodeToString(h.Sum(nil))
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.manifestPath(id), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestBackwardCompatReadsV1ObjectsWithoutMigration(t *testing.T) {
	root := t.TempDir()
	id := "legacy-obj"
	data := bytes.Repeat([]byte("legacy-bytes-"), 400) // ~5.2KB, 多块
	writeV1StoreOnDisk(t, root, id, data, 1024, 4)

	// 升级后打开：老目录没有 cas/tombstones/gc，Open 负责补齐。
	s, err := Open(root, Options{ChunkSize: 1024, Stripes: 4})
	if err != nil {
		t.Fatal(err)
	}
	m, err := s.loadManifest(id)
	if err != nil {
		t.Fatal(err)
	}
	if m.Version != 0 {
		t.Fatalf("legacy manifest version should be 0, got %d", m.Version)
	}

	// 无需迁移即可完整读取，逐块校验仍然生效。
	if err := s.Verify(id); err != nil {
		t.Fatalf("legacy verify (no migration): %v", err)
	}
	readObject(t, s, id, data)
	t.Logf("legacy v1 object %q read OK with zero migration; chunks still in private stripes", id)

	// 物理路径确实仍是老布局，而非 CAS。
	c0 := m.Chunks[1]
	if c0.Ref != "" {
		t.Fatal("legacy chunk must have empty ref")
	}
	if _, err := os.Stat(s.chunkPath(id, c0.Index)); errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("legacy private chunk missing: %v", err)
	}
	if _, err := os.Stat(s.chunkPathFor(m, c0)); errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("chunkPathFor must resolve legacy private path")
	}

	// 老对象去重重复提交仍幂等（不改变既有语义）。
	if err := s.Put(id, bytes.NewReader(data)); err != nil {
		t.Fatalf("idempotent recommit of legacy object: %v", err)
	}

	// 删除老对象：其独占私有块被直接释放，且可与"从未存在"区分。
	if err := s.Delete(id); err != nil {
		t.Fatal(err)
	}
	for i := range m.Chunks {
		if _, err := os.Stat(s.chunkPath(id, i)); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("legacy private chunk %d should be removed after delete", i)
		}
	}
	if _, err := s.Get(id); !errors.Is(err, ErrObjectDeleted) {
		t.Fatalf("deleted legacy object -> ErrObjectDeleted, got %v", err)
	}
	t.Logf("legacy object deleted: private chunks freed; conclusion still ErrObjectDeleted")

	// 新对象与老对象互不影响，且新对象走 v2 去重。
	if err := s.Put("fresh", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	mf, _ := s.loadManifest("fresh")
	if mf.Version != 2 {
		t.Fatalf("new object must be v2, got %d", mf.Version)
	}
	readObject(t, s, "fresh", data)
	t.Log("new writes after upgrade use CAS v2 layout; old and new formats coexist")
}

func TestResumeThenCommitUsesDedup(t *testing.T) {
	s := newStore(t)
	data := randBytes(t, 9*1024+77)
	// 写入中途崩溃，再 Resume 完成。
	if err := s.Put("res-dedup", &failAfterReader{r: bytes.NewReader(data), limit: 3*1024 + 50}); err == nil {
		t.Fatal("expected crash")
	}
	if err := s.Resume("res-dedup", bytes.NewReader(data)); err != nil {
		t.Fatalf("resume: %v", err)
	}
	m, err := s.loadManifest("res-dedup")
	if err != nil {
		t.Fatal(err)
	}
	if m.Version != 2 {
		t.Fatalf("resumed commit must land in CAS v2, got %d", m.Version)
	}
	for _, c := range m.Chunks {
		if c.Ref == "" || !s.casBlobExists(c.Ref, c.Length) {
			t.Fatalf("resumed chunk %d missing CAS ref %q", c.Index, c.Ref)
		}
	}
	readObject(t, s, "res-dedup", data)

	// 相同内容的另一个对象：所有块共享，CAS 块数不增加。
	before, _ := casFiles(t, s)
	if err := s.Put("res-copy", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	after, _ := casFiles(t, s)
	if len(after) != len(before) {
		t.Fatalf("identical resumed object must add zero blocks: %d -> %d", len(before), len(after))
	}
	readObject(t, s, "res-copy", data)
	t.Logf("resumed commit published to CAS; identical object shares all %d blocks (+0 physical)",
		len(m.Chunks))
}
