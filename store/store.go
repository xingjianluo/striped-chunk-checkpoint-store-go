package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

const (
	defaultChunkSize = 64 * 1024
	defaultStripes   = 4
)

// Store 是基于本地目录的分块条带存储。
type Store struct {
	root      string
	chunkSize int
	stripes   int
	// locks 为按对象 ID 的互斥锁，序列化同一对象的并发写入，
	// 保证"先到先得"的确定性提交结果。
	locks sync.Map
}

// lockObject 获取对象级互斥锁，返回解锁函数。
func (s *Store) lockObject(id string) func() {
	v, _ := s.locks.LoadOrStore(id, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// Options 控制 Store 行为。
type Options struct {
	// ChunkSize 为块大小（字节），默认 64KiB。
	ChunkSize int
	// Stripes 为条带目录数量，默认 4。
	Stripes int
}

// Open 创建或打开位于 root 的存储，并初始化目录结构。
func Open(root string, opts Options) (*Store, error) {
	s := &Store{root: root, chunkSize: opts.ChunkSize, stripes: opts.Stripes}
	if s.chunkSize <= 0 {
		s.chunkSize = defaultChunkSize
	}
	if s.stripes <= 0 {
		s.stripes = defaultStripes
	}
	dirs := []string{
		filepath.Join(root, "objects"),
		filepath.Join(root, "staging"),
	}
	for i := 0; i < s.stripes; i++ {
		dirs = append(dirs, filepath.Join(root, "data", fmt.Sprintf("stripe-%d", i)))
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Put 从头流式写入对象。内存占用恒定（一个块缓冲区），与对象大小无关。
func (s *Store) Put(id string, r io.Reader) error {
	unlock := s.lockObject(id)
	defer unlock()
	// 已提交：不落盘，仅流式比对内容摘要——一致则幂等成功，否则冲突。
	if m, err := s.loadManifest(id); err == nil {
		h := sha256.New()
		n, err := io.Copy(h, r)
		if err != nil {
			return err
		}
		if n == m.TotalSize && hex.EncodeToString(h.Sum(nil)) == m.Digest {
			return nil // 重复提交，幂等成功
		}
		return fmt.Errorf("%w: object %q already committed with different content",
			ErrCommitConflict, id)
	}
	if err := s.clearChunks(id); err != nil {
		return err
	}
	if err := s.removeCheckpoint(id); err != nil {
		return err
	}
	_, err := s.writeAll(id, r, 0, 0)
	return err
}

// Resume 依据检查点继续写入。
// 检查点损坏返回 ErrCheckpointCorrupt；磁盘上的块数多于检查点记录
// （检查点回退）返回 ErrCheckpointRegression，拒绝从中间继续。
func (s *Store) Resume(id string, r io.Reader) error {
	unlock := s.lockObject(id)
	defer unlock()
	if _, err := s.loadManifest(id); err == nil {
		return nil // 已提交，恢复幂等完成
	}
	cp, err := s.loadCheckpoint(id)
	if errors.Is(err, fs.ErrNotExist) {
		return s.Put(id, r) // 无检查点：等价于全新写入
	}
	if err != nil {
		return err // 损坏的检查点：明确拒绝
	}
	onDisk := s.countChunks(id)
	if onDisk > cp.NextChunk {
		return fmt.Errorf("%w: checkpoint at chunk %d but %d chunks on disk for %q",
			ErrCheckpointRegression, cp.NextChunk, onDisk, id)
	}
	// 跳过已写入的字节，恢复位置不回退。
	if _, err := io.CopyN(io.Discard, r, cp.BytesWritten); err != nil {
		return fmt.Errorf("resume %q: source shorter than checkpoint: %w", id, err)
	}
	_, err = s.writeAll(id, r, cp.NextChunk, cp.BytesWritten)
	return err
}

// writeAll 从第 startChunk 块开始流式写入，逐块落盘并推进检查点。
func (s *Store) writeAll(id string, r io.Reader, startChunk int, written int64) (*Manifest, error) {
	buf := make([]byte, s.chunkSize) // 唯一的块缓冲区，内存恒定
	objHash := sha256.New()
	chunks := make([]ChunkMeta, 0)

	// 恢复时把已有块的哈希纳入整体摘要。
	for i := 0; i < startChunk; i++ {
		data, err := os.ReadFile(s.chunkPath(id, i))
		if err != nil {
			return nil, fmt.Errorf("%w: chunk %d of %q: %v", ErrChunkMissing, i, id, err)
		}
		objHash.Write(data)
		chunks = append(chunks, ChunkMeta{
			Index: i, Stripe: s.stripeFor(i),
			Length: int64(len(data)), SHA256: sha256Hex(data),
		})
	}

	idx := startChunk
	total := written
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			meta, werr := s.writeChunk(id, idx, buf[:n])
			if werr != nil {
				return nil, werr
			}
			chunks = append(chunks, meta)
			objHash.Write(buf[:n])
			total += int64(n)
			idx++
			if cerr := s.saveCheckpoint(&Checkpoint{
				ObjectID: id, NextChunk: idx, BytesWritten: total,
			}); cerr != nil {
				return nil, cerr
			}
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	m := &Manifest{
		ObjectID:   id,
		ChunkSize:  s.chunkSize,
		TotalSize:  total,
		ChunkCount: len(chunks),
		Chunks:     chunks,
		Digest:     hex.EncodeToString(objHash.Sum(nil)),
	}
	if err := s.commitManifest(m); err != nil {
		return nil, err
	}
	if err := s.removeCheckpoint(id); err != nil {
		return nil, err
	}
	return m, nil
}

// writeChunk 原子写入单个块：临时文件 + rename + fsync。
func (s *Store) writeChunk(id string, index int, data []byte) (ChunkMeta, error) {
	path := s.chunkPath(id, index)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return ChunkMeta{}, err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return ChunkMeta{}, err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return ChunkMeta{}, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return ChunkMeta{}, err
	}
	if err := f.Close(); err != nil {
		return ChunkMeta{}, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return ChunkMeta{}, err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return ChunkMeta{}, err
	}
	return ChunkMeta{
		Index: index, Stripe: s.stripeFor(index),
		Length: int64(len(data)), SHA256: sha256Hex(data),
	}, nil
}

// countChunks 顺序统计磁盘上已存在的连续块数。
func (s *Store) countChunks(id string) int {
	n := 0
	for {
		if _, err := os.Stat(s.chunkPath(id, n)); err != nil {
			return n
		}
		n++
	}
}

// clearChunks 删除对象的全部块（用于全新写入前清理）。
func (s *Store) clearChunks(id string) error {
	for i := 0; i < s.stripes; i++ {
		dir := filepath.Join(s.root, "data", fmt.Sprintf("stripe-%d", i), id)
		if err := os.RemoveAll(dir); err != nil {
			return err
		}
	}
	return nil
}

// Get 返回已提交对象的流式读取器，逐块校验长度与内容。
// 校验失败时 Read 返回可区分的错误（ErrChunkMissing /
// ErrChunkLengthMismatch / ErrChecksumMismatch）。
func (s *Store) Get(id string) (io.ReadCloser, error) {
	m, err := s.loadManifest(id)
	if err != nil {
		return nil, err
	}
	return &chunkReader{s: s, m: m}, nil
}

// Verify 全量校验对象并返回可区分的失败原因。
func (s *Store) Verify(id string) error {
	m, err := s.loadManifest(id)
	if err != nil {
		return err
	}
	objHash := sha256.New()
	var total int64
	for _, c := range m.Chunks {
		data, err := os.ReadFile(s.chunkPath(id, c.Index))
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%w: chunk %d of %q", ErrChunkMissing, c.Index, id)
		}
		if err != nil {
			return err
		}
		if int64(len(data)) != c.Length {
			return fmt.Errorf("%w: chunk %d of %q: want %d bytes, got %d",
				ErrChunkLengthMismatch, c.Index, id, c.Length, len(data))
		}
		if got := sha256Hex(data); got != c.SHA256 {
			return fmt.Errorf("%w: chunk %d of %q", ErrChecksumMismatch, c.Index, id)
		}
		objHash.Write(data)
		total += int64(len(data))
	}
	if total != m.TotalSize || len(m.Chunks) != m.ChunkCount {
		return fmt.Errorf("%w: manifest totals inconsistent for %q", ErrChunkLengthMismatch, id)
	}
	if got := hex.EncodeToString(objHash.Sum(nil)); got != m.Digest {
		return fmt.Errorf("%w: object digest of %q", ErrChecksumMismatch, id)
	}
	return nil
}

// chunkReader 逐块流式读取：每次只在内存中保留一个已校验的块，
// 内存占用与对象大小无关。长度或内容校验失败的块不会被读出。
type chunkReader struct {
	s    *Store
	m    *Manifest
	next int
	cur  *bytes.Reader // 当前已校验块的内容
}

// loadNext 读取并校验下一块。
func (cr *chunkReader) loadNext() error {
	c := cr.m.Chunks[cr.next]
	path := cr.s.chunkPath(cr.m.ObjectID, c.Index)
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("%w: chunk %d of %q", ErrChunkMissing, c.Index, cr.m.ObjectID)
	}
	if err != nil {
		return err
	}
	if int64(len(data)) != c.Length {
		return fmt.Errorf("%w: chunk %d of %q: want %d bytes, got %d",
			ErrChunkLengthMismatch, c.Index, cr.m.ObjectID, c.Length, len(data))
	}
	if got := sha256Hex(data); got != c.SHA256 {
		return fmt.Errorf("%w: chunk %d of %q", ErrChecksumMismatch, c.Index, cr.m.ObjectID)
	}
	cr.cur = bytes.NewReader(data)
	cr.next++
	return nil
}

func (cr *chunkReader) Read(p []byte) (int, error) {
	for {
		if cr.cur == nil {
			if cr.next >= len(cr.m.Chunks) {
				return 0, io.EOF
			}
			if err := cr.loadNext(); err != nil {
				return 0, err
			}
		}
		n, err := cr.cur.Read(p)
		if err == io.EOF {
			cr.cur = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (cr *chunkReader) Close() error {
	cr.cur = nil
	return nil
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
