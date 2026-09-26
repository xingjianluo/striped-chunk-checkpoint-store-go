package chunkstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
)

// failAfter 在送达 failAt 字节后返回注入错误。
type failAfter struct {
	r      io.Reader
	failAt int64
	pos    int64
	failed bool
}

func (f *failAfter) Read(p []byte) (int, error) {
	if f.pos >= f.failAt {
		f.failed = true
		return 0, io.ErrUnexpectedEOF
	}
	lim := len(p)
	if int64(lim) > f.failAt-f.pos {
		lim = int(f.failAt - f.pos)
	}
	n, err := f.r.Read(p[:lim])
	f.pos += int64(n)
	return n, err
}

func reopenStore(t *testing.T, s *Store) *Store {
	t.Helper()
	s2, err := Open(s.root, s.cfg, WithLogger(log.New(os.Stdout, "  [reopen] ", log.LstdFlags)))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	return s2
}

func TestCheckpointResumeAfterCrash(t *testing.T) {
	s := testStore(t, 16, 3)
	content := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ") // 65
	total := int64(len(content))

	// 第一批：在写入 35 字节的位置中断（落在某个非块边界处）。
	u, err := s.BeginUpload("resume-me", total)
	if err != nil {
		t.Fatal(err)
	}
	fa := &failAfter{r: bytes.NewReader(content), failAt: 35}
	_, werr := u.CopyFrom(fa)
	if !errors.Is(werr, io.ErrUnexpectedEOF) {
		t.Fatalf("want injected EOF, got %v", werr)
	}
	crashOffset := u.Offset()
	uploadID := u.ID()
	// 检查点位置必须是块边界（整块才会推进检查点），即 32；
	// 不足一块的 3 字节随进程崩溃丢失，恢复时从 32 继续。
	if crashOffset != 32 {
		t.Fatalf("checkpoint offset=%d want 32", crashOffset)
	}
	fmt.Printf("中断点：upload=%s 检查点位置 offset=%d next_index=%d（未满块字节不落盘）\n",
		uploadID, crashOffset, crashOffset/16)

	// 模拟进程重启：重新 Open 同一根目录后恢复。
	s2 := reopenStore(t, s)
	u2, err := s2.ResumeUpload("resume-me")
	if err != nil {
		t.Fatalf("ResumeUpload: %v", err)
	}
	// 恢复位置不得回退。
	if u2.Offset() != crashOffset {
		t.Fatalf("resume offset=%d want %d", u2.Offset(), crashOffset)
	}
	// 从检查点位置继续写入剩余数据。
	if _, err := u2.CopyFrom(bytes.NewReader(content[crashOffset:])); err != nil {
		t.Fatalf("continue write: %v", err)
	}
	m, err := u2.Commit()
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if m.TotalSize != total {
		t.Fatalf("committed size=%d want %d", m.TotalSize, total)
	}

	// 读回校验内容完整一致。
	r, err := s2.OpenObject("resume-me")
	if err != nil {
		t.Fatalf("OpenObject: %v", err)
	}
	defer r.Close()
	got, _ := io.ReadAll(r)
	if !bytes.Equal(got, content) {
		t.Fatalf("content mismatch after resume: got %q", got)
	}
	fmt.Printf("恢复判定：从 offset=%d 继续，最终块数=%d 摘要=%s，内容一致\n",
		crashOffset, m.ChunkCount, m.Digest)

	// 提交后检查点应已删除，再次恢复必须报 ErrUploadNotFound。
	if _, err := s2.ResumeUpload("resume-me"); !errors.Is(err, ErrUploadNotFound) {
		t.Fatalf("post-commit resume want ErrUploadNotFound, got %v", err)
	}
}

// 损坏或不可信的检查点必须被明确拒绝。
func TestCheckpointTamperedRejected(t *testing.T) {
	s := testStore(t, 16, 2)
	content := make([]byte, 60)
	for i := range content {
		content[i] = byte(i)
	}
	u, err := s.BeginUpload("tampered", int64(len(content)))
	if err != nil {
		t.Fatal(err)
	}
	fa := &failAfter{r: bytes.NewReader(content), failAt: 33}
	_, _ = u.CopyFrom(fa)

	// 1) 篡改检查点文件内容（保持 JSON 外形）→ MAC 失配
	root := s.root
	cpPath := filepath.Join(root, checkpointsDir, u.ID()+".json")
	blob, err := os.ReadFile(cpPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cpPath, bytes.Replace(blob, []byte(`"next_index"`), []byte(`"next_indeX"`), 1), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = s.ResumeUpload("tampered")
	if !errors.Is(err, ErrCheckpointCorrupt) {
		t.Fatalf("tampered checkpoint: want ErrCheckpointCorrupt, got %v", err)
	}
	fmt.Printf("判定依据：检查点 MAC 失配 -> %v（拒绝从中间继续）\n", err)

	// 2) 恢复原检查点，但删除一个已完成块 → 不可信，拒绝
	if err := os.WriteFile(cpPath, blob, 0o600); err != nil {
		t.Fatal(err)
	}
	// 找出已写入的第一个块并删除
	m0, _ := s.readCheckpoint(u.ID())
	first := m0.Chunks[0]
	_ = os.Remove(s.chunkPath(first.Stripe, first.Hash))
	_, err = s.ResumeUpload("tampered")
	if !errors.Is(err, ErrCheckpointCorrupt) {
		t.Fatalf("missing chunk: want ErrCheckpointCorrupt, got %v", err)
	}
	fmt.Printf("判定依据：块 index=%d hash=%s 缺失，检查点与磁盘不符 -> %v\n",
		first.Index, first.Hash, err)
}

// 检查点位置只前进：同一读取器多次中断续传期间 offset 单调不减。
func TestCheckpointMonotonic(t *testing.T) {
	s := testStore(t, 8, 2)
	content := []byte("0123456789ABCDEF")
	u, err := s.BeginUpload("mono", int64(len(content)))
	if err != nil {
		t.Fatal(err)
	}
	// 每送达 3 字节注入一次中断；用同一读取器续传，
	// 已接收的字节不会重复喂入。
	fa := &failAfter{r: bytes.NewReader(content), failAt: 3}
	var prev int64
	step := 0
	for {
		_, werr := u.CopyFrom(fa)
		off := u.Offset()
		if off < prev {
			t.Fatalf("step %d checkpoint went backwards %d -> %d", step, prev, off)
		}
		fmt.Printf("步骤%d：检查点位置 offset=%d next_index=%d err=%v\n", step, off, u.nextIndex, werr)
		prev = off
		step++
		if werr == nil {
			break
		}
		if !errors.Is(werr, io.ErrUnexpectedEOF) {
			t.Fatalf("unexpected error: %v", werr)
		}
		// 模拟修复后重试：放宽中断点。
		fa.failAt += 3
	}
	m, err := u.Commit()
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.OpenObject("mono")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, _ := io.ReadAll(r)
	if !bytes.Equal(got, content) {
		t.Fatalf("content mismatch: got %q", got)
	}
	fmt.Printf("最终判定：块数=%d 摘要=%s 内容一致\n", m.ChunkCount, m.Digest)
}
