package chunkstore

import "encoding/json"

// HashLen 是内容哈希的字节长度（SHA-256）。
const HashLen = 32

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
// d0 = sha256("chunkstore-v1" || 0x00 || h0)
// d_i = sha256(d_{i-1} || 0x00 || h_i)
// 空对象的摘要为 sha256("chunkstore-v1")。
// 该方案无需序列化哈希内部状态即可在恢复点继续计算。
func chainDigest(hashes []string) string {
	return "" // TODO: 实现
}

// encodeManifest 生成清单的规范 JSON 编码。
func encodeManifest(m *Manifest) ([]byte, error) {
	return json.Marshal(m)
}

// decodeManifest 解析清单 JSON。
func decodeManifest(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
