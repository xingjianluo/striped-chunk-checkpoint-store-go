package chunkstore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// HashLen 是内容哈希的字节长度（SHA-256）。
const HashLen = 32

// digestDomain 是链式摘要的域分离前缀。
const digestDomain = "chunkstore-v1"

// ChunkRef 描述一个块在清单中的记录。
type ChunkRef struct {
	Index  int    `json:"index"`
	Stripe int    `json:"stripe"`
	Size   int64  `json:"size"`
	Hash   string `json:"hash"` // 小写十六进制 SHA-256
}

// Manifest 是对象提交后的不可变描述，读取与校验的依据。
type Manifest struct {
	Object     string     `json:"object"`
	ChunkSize  int64      `json:"chunk_size"`
	TotalSize  int64      `json:"total_size"`
	ChunkCount int        `json:"chunk_count"`
	Digest     string     `json:"digest"` // 链式摘要，见 chainDigest
	Chunks     []ChunkRef `json:"chunks"`
}

// chainDigest 由有序的块哈希计算对象级链式摘要：
//
//	d0   = sha256("chunkstore-v1" || 0x00 || h0)
//	d_i  = sha256(d_{i-1} || 0x00 || h_i)
//
// 空对象的摘要为 sha256("chunkstore-v1")。
// 该方案无需序列化哈希内部状态即可在恢复点继续计算，
// 且对块顺序与块边界敏感。
func chainDigest(hashes []string) string {
	sum := sha256.Sum256([]byte(digestDomain))
	for _, h := range hashes {
		raw, err := hex.DecodeString(h)
		if err != nil || len(raw) != HashLen {
			return ""
		}
		buf := make([]byte, 0, HashLen+1+HashLen)
		buf = append(buf, sum[:]...)
		buf = append(buf, 0x00)
		buf = append(buf, raw...)
		sum = sha256.Sum256(buf)
	}
	return hex.EncodeToString(sum[:])
}

// chunkHashes 提取清单中有序的块哈希列表。
func chunkHashes(refs []ChunkRef) []string {
	hs := make([]string, len(refs))
	for i, r := range refs {
		hs[i] = r.Hash
	}
	return hs
}

// encodeManifest 生成清单的规范 JSON 编码。
func encodeManifest(m *Manifest) ([]byte, error) {
	return json.Marshal(m)
}

// decodeManifest 解析清单 JSON。
func decodeManifest(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrManifestCorrupt, err)
	}
	return &m, nil
}

// validate 校验清单自身一致性（结构层面）。
func (m *Manifest) validate() error {
	if m.ChunkCount != len(m.Chunks) {
		return fmt.Errorf("%w: chunk_count=%d but %d chunk records",
			ErrManifestCorrupt, m.ChunkCount, len(m.Chunks))
	}
	if m.ChunkSize <= 0 {
		return fmt.Errorf("%w: non-positive chunk_size %d", ErrManifestCorrupt, m.ChunkSize)
	}
	var total int64
	for i, c := range m.Chunks {
		if c.Index != i {
			return fmt.Errorf("%w: chunk record %d has index %d",
				ErrManifestCorrupt, i, c.Index)
		}
		if c.Size <= 0 || c.Size > m.ChunkSize {
			return fmt.Errorf("%w: chunk %d size %d out of range",
				ErrManifestCorrupt, i, c.Size)
		}
		if i < len(m.Chunks)-1 && c.Size != m.ChunkSize {
			return fmt.Errorf("%w: non-final chunk %d has short size %d",
				ErrManifestCorrupt, i, c.Size)
		}
		if len(c.Hash) != HashLen*2 {
			return fmt.Errorf("%w: chunk %d hash malformed", ErrManifestCorrupt, i)
		}
		total += c.Size
	}
	if total != m.TotalSize {
		return fmt.Errorf("%w: total_size=%d but chunks sum to %d",
			ErrManifestCorrupt, m.TotalSize, total)
	}
	if m.Digest != chainDigest(chunkHashes(m.Chunks)) {
		return fmt.Errorf("%w: digest does not match chunk list", ErrManifestCorrupt)
	}
	return nil
}
