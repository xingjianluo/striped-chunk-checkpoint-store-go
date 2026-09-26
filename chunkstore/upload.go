package chunkstore

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
)

// maxTotal 是对象大小的硬上限，防止无界写入。
const maxTotal = 1 << 40 // 1 TiB

// Upload 是一次进行中的上传会话。
// 写入以块为单位流式进行，内存占用不超过一个块加固定缓冲。
// 每个块持久化后才推进检查点，崩溃后可用 ResumeUpload 继续。
type Upload struct {
	store     *Store
	id        string
	object    string
	total     int64 // 期望总大小，<0 表示未知
	mu        sync.Mutex
	buf       []byte // 未满一块的缓冲
	nextIndex int
	offset    int64
	chunks    []ChunkRef
	committed bool
	aborted   bool
}

// ID 返回上传会话标识。
func (u *Upload) ID() string { return u.id }

// Offset 返回已持久化的字节数（检查点位置）。
func (u *Upload) Offset() int64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.offset
}

// BeginUpload 开始一次新的上传会话。
// total 为期望的对象总大小，传负数表示未知（仅受 maxTotal 约束）。
func (s *Store) BeginUpload(object string, total int64) (*Upload, error) {
	if err := validateObjectName(object); err != nil {
		return nil, err
	}
	if total > maxTotal {
		return nil, fmt.Errorf("%w: %d exceeds limit %d", ErrTooLarge, total, maxTotal)
	}
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		return nil, err
	}
	u := &Upload{
		store:  s,
		id:     hex.EncodeToString(idBytes),
		object: object,
		total:  total,
		buf:    make([]byte, 0, s.cfg.ChunkSize),
	}
	if err := u.persistCheckpointLocked(); err != nil {
		return nil, err
	}
	s.log.Printf("upload begin: object=%s upload=%s checkpoint=(index=0 offset=0)",
		object, u.id)
	return u, nil
}

// ResumeUpload 依据检查点恢复上传会话。
// 检查点损坏或不可信时返回 ErrCheckpointCorrupt，绝不从中间继续；
// 无检查点返回 ErrUploadNotFound。
func (s *Store) ResumeUpload(object string) (*Upload, error) {
	if err := validateObjectName(object); err != nil {
		return nil, err
	}
	// 通过对象目录下的指针文件找到该对象最近的上传会话。
	uploadID, err := s.uploadIDFor(object)
	if err != nil {
		return nil, err
	}
	cp, err := s.readCheckpoint(uploadID)
	if err != nil {
		return nil, err
	}
	if cp.Object != object {
		return nil, fmt.Errorf("%w: checkpoint belongs to object %q, not %q",
			ErrCheckpointCorrupt, cp.Object, object)
	}
	if err := s.verifyCheckpointChunks(cp); err != nil {
		s.log.Printf("resume rejected: object=%s upload=%s reason=%v", object, uploadID, err)
		return nil, err
	}
	u := &Upload{
		store:     s,
		id:        cp.UploadID,
		object:    cp.Object,
		total:     cp.TotalLimit,
		buf:       make([]byte, 0, s.cfg.ChunkSize),
		nextIndex: cp.NextIndex,
		offset:    cp.NextOffset,
		chunks:    append([]ChunkRef(nil), cp.Chunks...),
	}
	s.log.Printf("upload resume: object=%s upload=%s checkpoint=(index=%d offset=%d) chunks_verified=%d",
		object, u.id, u.nextIndex, u.offset, len(u.chunks))
	return u, nil
}

// CopyFrom 从 r 流式读入数据，分块持久化并推进检查点。
// 返回实际接收的字节数；内存占用与对象大小无关。
// 读取出错时已持久化的进度保留在检查点中，可稍后继续。
func (u *Upload) CopyFrom(r io.Reader) (int64, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.checkUsableLocked(); err != nil {
		return 0, err
	}
	var received int64
	// 固定大小的读取缓冲，与对象大小无关。
	tmp := make([]byte, 32*1024)
	for {
		n, rerr := r.Read(tmp)
		if n > 0 {
			if err := u.feedLocked(tmp[:n]); err != nil {
				return received, err
			}
			received += int64(n)
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return received, nil
			}
			return received, rerr
		}
	}
}

// feedLocked 把数据喂入分块器，满块即落盘并推进检查点。
func (u *Upload) feedLocked(p []byte) error {
	for len(p) > 0 {
		need := int(u.store.cfg.ChunkSize) - len(u.buf)
		take := min(need, len(p))
		u.buf = append(u.buf, p[:take]...)
		p = p[take:]
		if int64(len(u.buf)) == u.store.cfg.ChunkSize {
			if err := u.flushChunkLocked(); err != nil {
				return err
			}
		}
	}
	return nil
}

// flushChunkLocked 把当前缓冲作为完整块落盘并推进检查点。
// 顺序保证：先写块数据，后推进检查点，恢复位置永不回退。
func (u *Upload) flushChunkLocked() error {
	if u.total >= 0 && u.offset+int64(len(u.buf)) > u.total {
		return fmt.Errorf("%w: declared total %d exceeded at offset %d",
			ErrTooLarge, u.total, u.offset)
	}
	if u.offset+int64(len(u.buf)) > maxTotal {
		return fmt.Errorf("%w: limit %d exceeded", ErrTooLarge, maxTotal)
	}
	sum := sha256.Sum256(u.buf)
	ref := ChunkRef{
		Index:  u.nextIndex,
		Stripe: u.nextIndex % u.store.cfg.Stripes,
		Size:   int64(len(u.buf)),
		Hash:   hex.EncodeToString(sum[:]),
	}
	written, err := u.store.writeChunk(ref.Stripe, ref.Hash, u.buf)
	if err != nil {
		return err
	}
	u.chunks = append(u.chunks, ref)
	u.nextIndex++
	u.offset += ref.Size
	u.buf = u.buf[:0]
	if err := u.persistCheckpointLocked(); err != nil {
		return err
	}
	if written {
		u.store.log.Printf("chunk written: object=%s upload=%s chunk=(index=%d stripe=%d hash=%s size=%d) checkpoint=(index=%d offset=%d)",
			u.object, u.id, ref.Index, ref.Stripe, ref.Hash, ref.Size, u.nextIndex, u.offset)
	} else {
		u.store.log.Printf("chunk dedup: object=%s upload=%s chunk=(index=%d stripe=%d hash=%s) already present, checkpoint=(index=%d offset=%d)",
			u.object, u.id, ref.Index, ref.Stripe, ref.Hash, u.nextIndex, u.offset)
	}
	return nil
}

// Commit 提交对象：冲刷尾块、校验期望总大小、原子发布清单。
// 并发提交同一对象时“先提交者获胜”；相同内容的重复提交幂等成功，
// 不同内容的提交返回 *ConflictError。提交后检查点被移除。
func (u *Upload) Commit() (*Manifest, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := u.checkUsableLocked(); err != nil {
		return nil, err
	}
	// 尾块（不足一块的剩余数据）落盘。
	if len(u.buf) > 0 {
		if err := u.flushChunkLocked(); err != nil {
			return nil, err
		}
	}
	if u.total >= 0 && u.offset != u.total {
		return nil, fmt.Errorf("%w: declared total %d but wrote %d",
			ErrChunkLengthMismatch, u.total, u.offset)
	}
	m := &Manifest{
		Object:     u.object,
		ChunkSize:  u.store.cfg.ChunkSize,
		TotalSize:  u.offset,
		ChunkCount: len(u.chunks),
		Digest:     chainDigest(chunkHashes(u.chunks)),
		Chunks:     append([]ChunkRef(nil), u.chunks...),
	}
	var result *Manifest
	err := u.store.withCommitLock(u.object, func() error {
		existing, err := u.store.readManifest(u.object)
		switch {
		case errors.Is(err, ErrObjectNotFound):
			// 无人提交过：发布清单。
			if err := u.store.writeManifestAtomic(m); err != nil {
				return err
			}
			u.store.log.Printf("commit: object=%s upload=%s digest=%s chunks=%d size=%d verdict=published",
				u.object, u.id, m.Digest, m.ChunkCount, m.TotalSize)
			result = m
			return nil
		case err != nil:
			return err
		}
		// 已存在提交：按“先提交者获胜”仲裁。
		if existing.Digest == m.Digest {
			u.store.log.Printf("commit: object=%s upload=%s digest=%s verdict=idempotent-replay (matches existing)",
				u.object, u.id, m.Digest)
			result = existing
			return nil
		}
		u.store.log.Printf("commit: object=%s upload=%s digest=%s verdict=conflict existing=%s (first-commit-wins)",
			u.object, u.id, m.Digest, existing.Digest)
		result = existing
		return &ConflictError{
			Object:         u.object,
			ExistingDigest: existing.Digest,
			AttemptDigest:  m.Digest,
		}
	})
	if err != nil {
		return result, err
	}
	u.committed = true
	if err := u.store.removeCheckpoint(u.id); err != nil {
		return result, err
	}
	u.store.clearUploadPointer(u.object, u.id)
	return result, nil
}

// Abort 中止上传并删除检查点；已写入的块保留供后续会话复用。
func (u *Upload) Abort() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.committed {
		return ErrCommitted
	}
	if u.aborted {
		return nil
	}
	u.aborted = true
	u.store.clearUploadPointer(u.object, u.id)
	return u.store.removeCheckpoint(u.id)
}

// PutObject 以单调用方式写入并提交对象（内存占用有界）。
func (s *Store) PutObject(object string, r io.Reader) (*Manifest, error) {
	u, err := s.BeginUpload(object, -1)
	if err != nil {
		return nil, err
	}
	if _, err := u.CopyFrom(r); err != nil {
		u.Abort()
		return nil, err
	}
	return u.Commit()
}

// checkUsableLocked 校验会话仍可写入。
func (u *Upload) checkUsableLocked() error {
	if u.committed {
		return ErrCommitted
	}
	if u.aborted {
		return ErrAborted
	}
	return nil
}

// persistCheckpointLocked 把当前进度写入检查点。
// 只在块数据持久化之后调用。
func (u *Upload) persistCheckpointLocked() error {
	cp := &Checkpoint{
		Version:    checkpointVersion,
		UploadID:   u.id,
		Object:     u.object,
		ChunkSize:  u.store.cfg.ChunkSize,
		Stripes:    u.store.cfg.Stripes,
		NextIndex:  u.nextIndex,
		NextOffset: u.offset,
		TotalLimit: u.total,
		Digest:     chainDigest(chunkHashes(u.chunks)),
		Chunks:     append([]ChunkRef(nil), u.chunks...),
	}
	if err := u.store.writeCheckpoint(cp); err != nil {
		return err
	}
	return u.store.setUploadPointer(u.object, u.id)
}
