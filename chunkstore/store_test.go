package chunkstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testStore(t *testing.T, chunkSize int64, stripes int) *Store {
	t.Helper()
	root := t.TempDir()
	s, err := Open(root, Config{ChunkSize: chunkSize, Stripes: stripes},
		WithLogger(log.New(os.Stdout, "  [store] ", log.LstdFlags)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

// newReader 生成确定性可重复的数据内容。
func dataReader(n int64, seed byte) io.Reader {
	return &patternReader{size: n, seed: seed}
}

type patternReader struct {
	size int64
	seed byte
	pos  int64
}

func (p *patternReader) Read(b []byte) (int, error) {
	if p.pos >= p.size {
		return 0, io.EOF
	}
	n := int64(len(b))
	if p.pos+n > p.size {
		n = p.size - p.pos
	}
	for i := int64(0); i < n; i++ {
		b[i] = byte(p.pos+i)*7 + p.seed
	}
	p.pos += n
	return int(n), nil
}

func readAll(t *testing.T, r io.Reader) []byte {
	t.Helper()
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read object: %v", err)
	}
	return out
}

func TestBasicRoundTrip(t *testing.T) {
	s := testStore(t, 17, 4) // 故意小块 + 非整除
	var want bytes.Buffer
	io.Copy(&want, dataReader(100, 3))

	m, err := s.PutObject("obj-a", bytes.NewReader(want.Bytes()))
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if m.ChunkCount != 6 || m.TotalSize != 100 { // 17*5+15
		t.Fatalf("manifest chunks=%d size=%d", m.ChunkCount, m.TotalSize)
	}
	// 检查条带分布：块下标按 stripes 取模。
	for _, c := range m.Chunks {
		if c.Stripe != c.Index%4 {
			t.Fatalf("chunk %d on stripe %d", c.Index, c.Stripe)
		}
	}

	r, err := s.OpenObject("obj-a")
	if err != nil {
		t.Fatalf("OpenObject: %v", err)
	}
	defer r.Close()
	got := readAll(t, r)
	if !bytes.Equal(got, want.Bytes()) {
		t.Fatalf("content mismatch (got %d bytes, want %d)", len(got), want.Len())
	}

	// 重复打开不存在的对象
	if _, err := s.OpenObject("nope"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("want ErrObjectNotFound, got %v", err)
	}
}

func TestEmptyObject(t *testing.T) {
	s := testStore(t, 16, 2)
	m, err := s.PutObject("empty", strings.NewReader(""))
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if m.TotalSize != 0 || m.ChunkCount != 0 {
		t.Fatalf("empty object manifest=%+v", m)
	}
	r, err := s.OpenObject("empty")
	if err != nil {
		t.Fatalf("OpenObject: %v", err)
	}
	defer r.Close()
	if b := readAll(t, r); len(b) != 0 {
		t.Fatalf("want empty, got %d bytes", len(b))
	}
}

// 破坏失败原因可区分：块缺失 / 长度不符 / 内容哈希不符。
func TestChunkFailureReasons(t *testing.T) {
	s := testStore(t, 16, 3)
	content := []byte(strings.Repeat("abcdefghijklmnopqrstuvwxyz", 4)) // 104 字节
	m, err := s.PutObject("obj", bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	// 1) 块内容被改写 → 内容校验失败
	c := m.Chunks[2]
	path := s.chunkPath(c.Stripe, c.Hash)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[0] ^= 0xFF
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	err = s.VerifyObject("obj")
	var ce *ChunkError
	if !errors.As(err, &ce) || !errors.Is(err, ErrChunkChecksumMismatch) {
		t.Fatalf("want *ChunkError{ErrChunkChecksumMismatch}, got %v", err)
	}
	fmt.Printf("判定依据：块 index=%d hash=%s -> %v\n", ce.Index, ce.Hash, ce.Err)
	// 恢复
	raw[0] ^= 0xFF
	os.WriteFile(path, raw, 0o644)
	if err := s.VerifyObject("obj"); err != nil {
		t.Fatalf("repaired verify: %v", err)
	}

	// 2) 块长度不符：用同哈希名下写入截断内容 → 先触发长度不符
	if err := os.Truncate(path, c.Size-1); err != nil {
		t.Fatal(err)
	}
	err = s.VerifyObject("obj")
	if !errors.As(err, &ce) || !errors.Is(err, ErrChunkLengthMismatch) {
		t.Fatalf("want ErrChunkLengthMismatch, got %v", err)
	}
	fmt.Printf("判定依据：块 index=%d hash=%s expected=%d -> %v\n", ce.Index, ce.Hash, c.Size, ce.Err)

	// 3) 块缺失
	_ = os.WriteFile(path, raw, 0o644)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	err = s.VerifyObject("obj")
	if !errors.As(err, &ce) || !errors.Is(err, ErrChunkMissing) {
		t.Fatalf("want ErrChunkMissing, got %v", err)
	}
	fmt.Printf("判定依据：块 index=%d hash=%s 路径=%s -> %v\n", ce.Index, ce.Hash, filepath.Base(path), ce.Err)
}
