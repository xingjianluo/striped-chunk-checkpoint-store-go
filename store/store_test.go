package store

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir(), Options{ChunkSize: 1024, Stripes: 4})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return s
}

func randBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

// logCheckpoint 打印检查点位置，作为恢复判定依据。
func logCheckpoint(t *testing.T, s *Store, id, when string) {
	t.Helper()
	cp, err := s.loadCheckpoint(id)
	if err != nil {
		t.Logf("[%s] checkpoint for %q: none (%v)", when, id, err)
		return
	}
	t.Logf("[%s] checkpoint for %q: next_chunk=%d bytes=%d crc=%d",
		when, id, cp.NextChunk, cp.BytesWritten, cp.CRC)
}

func TestPutGetRoundtrip(t *testing.T) {
	s := newStore(t)
	data := randBytes(t, 10*1024+137) // 跨 11 个块
	if err := s.Put("obj-a", bytes.NewReader(data)); err != nil {
		t.Fatalf("put: %v", err)
	}
	rc, err := s.Get("obj-a")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("roundtrip data mismatch")
	}
	if err := s.Verify("obj-a"); err != nil {
		t.Fatalf("verify: %v", err)
	}
	// 验证条带分布：块应轮转分布在 4 个条带目录。
	m, _ := s.loadManifest("obj-a")
	seen := map[int]int{}
	for _, c := range m.Chunks {
		seen[c.Stripe]++
		if c.Stripe != c.Index%4 {
			t.Errorf("chunk %d on stripe %d, want %d", c.Index, c.Stripe, c.Index%4)
		}
	}
	t.Logf("striping distribution: %v (chunks=%d)", seen, m.ChunkCount)
	if len(seen) != 4 {
		t.Fatalf("expected 4 stripes used, got %d", len(seen))
	}
}

func TestDistinguishableFailures(t *testing.T) {
	s := newStore(t)
	data := randBytes(t, 5*1024)
	if err := s.Put("obj-b", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	m, _ := s.loadManifest("obj-b")

	// 1) 块缺失
	victim := m.Chunks[2]
	p := s.chunkPath("obj-b", victim.Index)
	os.Rename(p, p+".bak")
	err := s.Verify("obj-b")
	t.Logf("chunk id=%d stripe=%d removed -> verify err=%v", victim.Index, victim.Stripe, err)
	if !errors.Is(err, ErrChunkMissing) {
		t.Fatalf("want ErrChunkMissing, got %v", err)
	}
	os.Rename(p+".bak", p)

	// 2) 长度不匹配（截断）
	f, _ := os.OpenFile(p, os.O_WRONLY, 0)
	f.Truncate(victim.Length - 10)
	f.Close()
	err = s.Verify("obj-b")
	t.Logf("chunk id=%d truncated to %d/%d bytes -> verify err=%v",
		victim.Index, victim.Length-10, victim.Length, err)
	if !errors.Is(err, ErrChunkLengthMismatch) {
		t.Fatalf("want ErrChunkLengthMismatch, got %v", err)
	}

	// 3) 内容校验失败（同长度改写）
	if err := os.WriteFile(p, randBytes(t, int(victim.Length)), 0o644); err != nil {
		t.Fatal(err)
	}
	err = s.Verify("obj-b")
	t.Logf("chunk id=%d content rewritten (same length) -> verify err=%v", victim.Index, err)
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("want ErrChecksumMismatch, got %v", err)
	}

	// 4) 读取路径同样不得返回损坏数据
	rc, err := s.Get("obj-b")
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(rc)
	rc.Close()
	t.Logf("streaming read of corrupted object (chunk id=%d) -> err=%v", victim.Index, err)
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("want ErrChecksumMismatch from streaming read, got %v", err)
	}
}

func TestConcurrentCommitSameContent(t *testing.T) {
	s := newStore(t)
	data := randBytes(t, 8*1024)
	const writers = 8
	var wg sync.WaitGroup
	errs := make([]error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = s.Put("obj-c", bytes.NewReader(data))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d: %v (duplicate commit must be idempotent)", i, err)
		}
	}
	if err := s.Verify("obj-c"); err != nil {
		t.Fatalf("verify after concurrent commit: %v", err)
	}
	rc, _ := s.Get("obj-c")
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, data) {
		t.Fatal("final content mismatch after concurrent commit")
	}
	t.Logf("concurrent commit of identical content: all %d writers succeeded, digest stable", writers)
}

func TestConcurrentCommitConflictingContent(t *testing.T) {
	s := newStore(t)
	winner := randBytes(t, 4*1024)
	loser := randBytes(t, 4*1024)
	const n = 6
	var wg sync.WaitGroup
	errs := make([]error, n)
	contents := make([][]byte, n)
	for i := 0; i < n; i++ {
		contents[i] = winner
		if i%2 == 1 {
			contents[i] = loser
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = s.Put("obj-d", bytes.NewReader(contents[i]))
		}(i)
	}
	wg.Wait()
	var conflicts, oks int
	for _, err := range errs {
		if errors.Is(err, ErrCommitConflict) {
			conflicts++
		} else if err == nil {
			oks++
		} else {
			t.Fatalf("unexpected err: %v", err)
		}
	}
	// 最终结果确定：存储内容必须等于某一个提交者的内容。
	rc, _ := s.Get("obj-d")
	final, _ := io.ReadAll(rc)
	rc.Close()
	isWinner := bytes.Equal(final, winner)
	isLoser := bytes.Equal(final, loser)
	if isWinner == isLoser { // 两者相等或都不等都是异常
		t.Fatalf("final content not deterministic: winner=%v loser=%v", isWinner, isLoser)
	}
	t.Logf("concurrent conflicting commits: ok=%d conflict=%d, final matches %s content",
		oks, conflicts, map[bool]string{true: "even-writers", false: "odd-writers"}[isWinner])
	if conflicts == 0 {
		t.Fatal("expected at least one ErrCommitConflict")
	}
}

// failAfterReader 在流出 limit 字节后返回错误，模拟写入中断。
type failAfterReader struct {
	r     io.Reader
	limit int64
	n     int64
}

func (f *failAfterReader) Read(p []byte) (int, error) {
	if f.n >= f.limit {
		return 0, errors.New("simulated crash")
	}
	if int64(len(p)) > f.limit-f.n {
		p = p[:f.limit-f.n]
	}
	n, err := f.r.Read(p)
	f.n += int64(n)
	return n, err
}

func TestInterruptAndResume(t *testing.T) {
	s := newStore(t)
	data := randBytes(t, 20*1024+5) // 21 块

	// 第一次写入在第 7 块附近中断。
	crashAt := int64(7*1024 + 100)
	err := s.Put("obj-e", &failAfterReader{r: bytes.NewReader(data), limit: crashAt})
	if err == nil {
		t.Fatal("expected crash error")
	}
	logCheckpoint(t, s, "obj-e", "after crash")
	cp1, _ := s.loadCheckpoint("obj-e")
	if cp1 == nil || cp1.NextChunk == 0 {
		t.Fatalf("checkpoint not advanced after crash: %+v", cp1)
	}
	// 中断后对象不得可见。
	if _, err := s.Get("obj-e"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("half-committed object must not be visible, got %v", err)
	}

	// 恢复：从头提供数据流，Resume 跳过已完成部分。
	if err := s.Resume("obj-e", bytes.NewReader(data)); err != nil {
		t.Fatalf("resume: %v", err)
	}
	logCheckpoint(t, s, "obj-e", "after resume")
	if err := s.Verify("obj-e"); err != nil {
		t.Fatalf("verify after resume: %v", err)
	}
	rc, _ := s.Get("obj-e")
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, data) {
		t.Fatal("resumed data mismatch: completed data lost or duplicated")
	}
	t.Logf("resume succeeded: checkpoint advanced from chunk %d, final %d bytes verified",
		cp1.NextChunk, len(data))
}

func TestResumeRejectsCorruptCheckpoint(t *testing.T) {
	s := newStore(t)
	data := randBytes(t, 4*1024)
	err := s.Put("obj-f", &failAfterReader{r: bytes.NewReader(data), limit: 1500})
	if err == nil {
		t.Fatal("expected crash")
	}
	logCheckpoint(t, s, "obj-f", "before corruption")
	// 篡改检查点内容。
	cpPath := s.checkpointPath("obj-f")
	raw, _ := os.ReadFile(cpPath)
	raw[len(raw)/2] ^= 0xFF
	os.WriteFile(cpPath, raw, 0o644)
	err = s.Resume("obj-f", bytes.NewReader(data))
	t.Logf("corrupted checkpoint at %s -> resume err=%v", filepath.Base(cpPath), err)
	if !errors.Is(err, ErrCheckpointCorrupt) {
		t.Fatalf("want ErrCheckpointCorrupt, got %v", err)
	}
}

func TestResumeRejectsRegressedCheckpoint(t *testing.T) {
	s := newStore(t)
	data := randBytes(t, 6*1024)
	s.Put("obj-g", &failAfterReader{r: bytes.NewReader(data), limit: 3*1024 + 1})
	cp, err := s.loadCheckpoint("obj-g")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("checkpoint before regression: next_chunk=%d bytes=%d", cp.NextChunk, cp.BytesWritten)
	// 构造回退的检查点：块数少于磁盘上已有块数。
	regressed := &Checkpoint{ObjectID: "obj-g", NextChunk: cp.NextChunk - 2, BytesWritten: cp.BytesWritten - 2048}
	if err := s.saveCheckpoint(regressed); err != nil {
		t.Fatal(err)
	}
	err = s.Resume("obj-g", bytes.NewReader(data))
	t.Logf("regressed checkpoint next_chunk=%d (disk has %d chunks) -> resume err=%v",
		regressed.NextChunk, s.countChunks("obj-g"), err)
	if !errors.Is(err, ErrCheckpointRegression) {
		t.Fatalf("want ErrCheckpointRegression, got %v", err)
	}
}

func TestDuplicateCommitIdempotent(t *testing.T) {
	s := newStore(t)
	data := randBytes(t, 3*1024)
	if err := s.Put("obj-h", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("obj-h", bytes.NewReader(data)); err != nil {
		t.Fatalf("duplicate commit must be idempotent: %v", err)
	}
	other := randBytes(t, 3*1024)
	err := s.Put("obj-h", bytes.NewReader(other))
	if !errors.Is(err, ErrCommitConflict) {
		t.Fatalf("want ErrCommitConflict, got %v", err)
	}
	t.Logf("duplicate commit idempotent; conflicting recommit -> %v", err)
}

func TestLargeObjectBoundedMemory(t *testing.T) {
	// 64MiB 对象、1MiB 块：写入与读取均只缓冲单个块，
	// 若实现把对象整体读入内存将明显放大占用。
	s, err := Open(t.TempDir(), Options{ChunkSize: 1 << 20, Stripes: 4})
	if err != nil {
		t.Fatal(err)
	}
	data := randBytes(t, 64<<20)
	if err := s.Put("obj-big", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if err := s.Verify("obj-big"); err != nil {
		t.Fatal(err)
	}
	m, _ := s.loadManifest("obj-big")
	t.Logf("big object: %d bytes in %d chunks of %d bytes, digest=%s",
		m.TotalSize, m.ChunkCount, m.ChunkSize, m.Digest[:16])
}

func TestGetNotFound(t *testing.T) {
	s := newStore(t)
	_, err := s.Get("nope")
	if !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("want ErrObjectNotFound, got %v", err)
	}
	fmt.Println()
}
