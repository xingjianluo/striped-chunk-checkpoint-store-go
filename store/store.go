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
	// life 为生命周期读写锁：读/提交窗口持读锁，Delete/Reclaim 持写锁，
	// 确保正在读取或提交所依赖的数据不会在回收/删除期间被移除。
	life sync.RWMutex
	// gcMu 串行化进程内的回收调用，避免多个 Reclaim 争抢同一计划文件。
	gcMu sync.Mutex
	// onSweepBlock 在每个计划项删除前被调用（测试钩子，用于注入中断）。
	onSweepBlock func(ref string)
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
		filepath.Join(root, "cas", "sha256"),
		filepath.Join(root, "tombstones"),
		filepath.Join(root, "gc"),
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
	} else if errors.Is(err, ErrObjectDeleted) {
		// 已删除的对象 ID 不会被新内容覆盖，保证删除结论明确可区分。
		return fmt.Errorf("%w: cannot overwrite deleted object %q", ErrObjectDeleted, id)
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
	} else if errors.Is(err, ErrObjectDeleted) {
		return fmt.Errorf("%w: cannot resume deleted object %q", ErrObjectDeleted, id)
	}
	cp, err := s.loadCheckpoint(id)
	if errors.Is(err, fs.ErrNotExist) {
		return s.Put(id, r) // 无检查点：等价于全新写入
	}
	if err != nil {
		return err // 损坏的检查点：明确拒绝
	}
	onDisk := s.countChunks(id)
	fullOnDisk := s.countFullChunks(id)
	if fullOnDisk > cp.NextChunk {
		return fmt.Errorf("%w: checkpoint at chunk %d but %d full chunks on disk for %q",
			ErrCheckpointRegression, cp.NextChunk, fullOnDisk, id)
	}
	// 丢弃崩溃遗留的尾部碎片（位于检查点位置的不足块），保证恢复后
	// 的块边界与全新写入一致。
	if onDisk > cp.NextChunk {
		if err := os.Remove(s.chunkPath(id, cp.NextChunk)); err != nil &&
			!errors.Is(err, fs.ErrNotExist) {
			return err
		}
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
		switch {
		case err == nil:
			// 完整块：落盘并推进检查点。完整块边界在恢复后保持不变，
			// 因此中断后续写与全新写入产出逐字节相同的块哈希，可正确去重。
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
		case err == io.ErrUnexpectedEOF && n > 0:
			// 最后一个不足块（EOF 前的尾部碎片）：写入但不推进检查点。
			// 若在此处崩溃，该碎片不属于任何已确认块，恢复时会被丢弃重写，
			// 从而保证块边界不被崩溃位置影响。
			meta, werr := s.writeChunk(id, idx, buf[:n])
			if werr != nil {
				return nil, werr
			}
			chunks = append(chunks, meta)
			objHash.Write(buf[:n])
			total += int64(n)
		case err == io.EOF:
			// 恰好以块边界结束。
		default:
			return nil, err
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
	}

	m := &Manifest{
		Version:    2,
		ObjectID:   id,
		ChunkSize:  s.chunkSize,
		TotalSize:  total,
		ChunkCount: len(chunks),
		Chunks:     chunks,
		Digest:     hex.EncodeToString(objHash.Sum(nil)),
	}
	// 提交窗口：提升块到 CAS 并发布清单，全程持生命周期读锁，
	// 使 Reclaim 不可能在"块已进 CAS、清单尚未可见"的间隙误删它。
	s.life.RLock()
	defer s.life.RUnlock()
	if err := s.publishChunks(m); err != nil {
		return nil, err
	}
	if err := s.commitManifest(m); err != nil {
		return nil, err
	}
	if err := s.removeCheckpoint(id); err != nil {
		return nil, err
	}
	// 清单已发布，CAS 为块的权威位置；对象私有暂存块随即释放。
	if err := s.clearChunks(id); err != nil {
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

// countChunks 顺序统计磁盘上已存在的连续块数（含不足整的尾部碎片）。
func (s *Store) countChunks(id string) int {
	n := 0
	for {
		if _, err := os.Stat(s.chunkPath(id, n)); err != nil {
			return n
		}
		n++
	}
}

// countFullChunks 统计连续存在且大小恰好为 ChunkSize 的块数。
// 末尾不足整的碎片不计入，用于检查点回退判定。
func (s *Store) countFullChunks(id string) int {
	n := 0
	for {
		fi, err := os.Stat(s.chunkPath(id, n))
		if err != nil || fi.Size() != int64(s.chunkSize) {
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
// 读取器持有生命周期读锁，在 Close 之前 Reclaim/Delete 不会移除
// 它所依赖的共享块；Close 必须调用。
// 校验失败时 Read 返回可区分的错误（ErrChunkMissing /
// ErrChunkLengthMismatch / ErrChecksumMismatch）。
func (s *Store) Get(id string) (io.ReadCloser, error) {
	s.life.RLock()
	m, err := s.loadManifest(id)
	if err != nil {
		s.life.RUnlock()
		return nil, err
	}
	return &chunkReader{s: s, m: m}, nil
}

// newReaderNoLock 构造一个不参与生命周期锁的读取器（不加锁、Close /
// 读出错时也不解锁），供调用方自行持有/释放读窗口的内部与测试路径。
func (s *Store) newReaderNoLock(id string) (io.ReadCloser, error) {
	m, err := s.loadManifest(id)
	if err != nil {
		return nil, err
	}
	return &chunkReader{s: s, m: m, noLock: true}, nil
}

// Verify 全量校验对象并返回可区分的失败原因。
func (s *Store) Verify(id string) error {
	s.life.RLock()
	defer s.life.RUnlock()
	m, err := s.loadManifest(id)
	if err != nil {
		return err
	}
	objHash := sha256.New()
	var total int64
	for _, c := range m.Chunks {
		data, err := os.ReadFile(s.chunkPathFor(m, c))
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
// 持有 Store 的生命周期读锁，直到 Close。
type chunkReader struct {
	s      *Store
	m      *Manifest
	next   int
	cur    *bytes.Reader // 当前已校验块的内容
	closed bool
	// noLock 为 true 时读取器不持有生命周期读锁（读窗口由外部管理），
	// Close 或读出错时不执行 RUnlock。
	noLock bool
}

// loadNext 读取并校验下一块。
func (cr *chunkReader) loadNext() error {
	c := cr.m.Chunks[cr.next]
	path := cr.s.chunkPathFor(cr.m, c)
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
				// 出错时调用方可能不再 Close：主动释放一次生命周期读锁，
				// 防止 Reclaim/Delete 被永久阻塞。Close 仍可安全重复调用。
				if !cr.closed && !cr.noLock {
					cr.closed = true
					cr.s.life.RUnlock()
				}
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
	if cr.closed {
		return nil
	}
	cr.closed = true
	cr.cur = nil
	if !cr.noLock {
		cr.s.life.RUnlock()
	}
	return nil
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
