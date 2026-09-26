package chunkstore

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
)

// Reader 按序读取已提交对象的内容。
// 打开时已通过完整性校验，读取过程内存占用有界。
type Reader struct {
	manifest *Manifest
	store    *Store
	pos      int64
	chunkIdx int
	cur      io.ReadCloser
	curOff   int64 // 当前块内已读偏移
}

// Read 实现 io.Reader。
func (r *Reader) Read(p []byte) (int, error) {
	if r.chunkIdx >= len(r.manifest.Chunks) {
		return 0, io.EOF
	}
	if r.cur == nil {
		c := r.manifest.Chunks[r.chunkIdx]
		f, err := os.Open(r.store.chunkPath(c.Stripe, c.Hash))
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return 0, &ChunkError{Object: r.manifest.Object, Index: c.Index, Hash: c.Hash, Err: ErrChunkMissing}
			}
			return 0, err
		}
		r.cur = f
		r.curOff = 0
	}
	n, err := r.cur.Read(p)
	r.pos += int64(n)
	r.curOff += int64(n)
	if errors.Is(err, io.EOF) {
		c := r.manifest.Chunks[r.chunkIdx]
		if r.curOff != c.Size {
			r.cur.Close()
			r.cur = nil
			return n, &ChunkError{Object: r.manifest.Object, Index: c.Index, Hash: c.Hash, Err: ErrChunkLengthMismatch}
		}
		r.cur.Close()
		r.cur = nil
		r.chunkIdx++
		if n > 0 {
			return n, nil
		}
		return r.Read(p)
	}
	return n, err
}

// Close 释放读取资源。
func (r *Reader) Close() error {
	if r.cur != nil {
		err := r.cur.Close()
		r.cur = nil
		return err
	}
	return nil
}

// Size 返回对象总大小。
func (r *Reader) Size() int64 { return r.manifest.TotalSize }

// Manifest 返回对象清单。
func (r *Reader) Manifest() *Manifest { return r.manifest }

// OpenObject 打开已提交对象进行流式读取。
// 打开时做完整性校验，失败原因可区分：
//   - ErrObjectNotFound       无已提交清单
//   - ErrManifestCorrupt      清单损坏/自相矛盾
//   - ErrChunkMissing         清单引用的块缺失
//   - ErrChunkLengthMismatch  块长度与清单不符
//   - ErrChunkChecksumMismatch 块内容哈希不符（统一包装为 *ChunkError）
func (s *Store) OpenObject(object string) (*Reader, error) {
	if err := validateObjectName(object); err != nil {
		return nil, err
	}
	m, err := s.readManifest(object)
	if err != nil {
		return nil, err
	}
	if err := s.verifyChunks(object, m); err != nil {
		return nil, err
	}
	return &Reader{manifest: m, store: s}, nil
}

// VerifyObject 校验对象完整性而不读出数据。
func (s *Store) VerifyObject(object string) error {
	if err := validateObjectName(object); err != nil {
		return err
	}
	m, err := s.readManifest(object)
	if err != nil {
		return err
	}
	return s.verifyChunks(object, m)
}

// ManifestOf 返回对象的已提交清单。
func (s *Store) ManifestOf(object string) (*Manifest, error) {
	if err := validateObjectName(object); err != nil {
		return nil, err
	}
	return s.readManifest(object)
}

// verifyChunks 逐块核对存在性、长度与内容哈希。
// 判定顺序：缺失 → 长度不符 → 内容校验失败，错误原因可区分。
func (s *Store) verifyChunks(object string, m *Manifest) error {
	var total int64
	for _, c := range m.Chunks {
		ce := func(err error) *ChunkError {
			return &ChunkError{Object: object, Index: c.Index, Hash: c.Hash, Err: err}
		}
		f, err := os.Open(s.chunkPath(c.Stripe, c.Hash))
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				s.log.Printf("verify: object=%s chunk=(index=%d stripe=%d hash=%s) verdict=missing",
					object, c.Index, c.Stripe, c.Hash)
				return ce(ErrChunkMissing)
			}
			return err
		}
		h := sha256.New()
		n, err := io.CopyBuffer(h, f, make([]byte, 32*1024))
		f.Close()
		if err != nil {
			return err
		}
		if n != c.Size {
			s.log.Printf("verify: object=%s chunk=(index=%d stripe=%d hash=%s) verdict=length-mismatch expected=%d actual=%d",
				object, c.Index, c.Stripe, c.Hash, c.Size, n)
			return ce(ErrChunkLengthMismatch)
		}
		got := hex.EncodeToString(h.Sum(nil))
		if got != c.Hash {
			s.log.Printf("verify: object=%s chunk=(index=%d stripe=%d hash=%s) verdict=checksum-mismatch actual=%s",
				object, c.Index, c.Stripe, c.Hash, got)
			return ce(ErrChunkChecksumMismatch)
		}
		total += n
		s.log.Printf("verify: object=%s chunk=(index=%d stripe=%d hash=%s size=%d) verdict=ok",
			object, c.Index, c.Stripe, c.Hash, n)
	}
	if total != m.TotalSize {
		return errors.Join(ErrChunkLengthMismatch, io.ErrUnexpectedEOF)
	}
	s.log.Printf("verify: object=%s verdict=ok chunks=%d size=%d digest=%s",
		object, m.ChunkCount, total, m.Digest)
	return nil
}
