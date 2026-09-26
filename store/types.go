package store

// ChunkMeta 描述单个块在清单中的记录。
type ChunkMeta struct {
	Index  int    `json:"index"`
	Stripe int    `json:"stripe"`
	Length int64  `json:"length"`
	SHA256 string `json:"sha256"`
}

// Manifest 是对象提交后的不可变元数据。
type Manifest struct {
	ObjectID   string      `json:"object_id"`
	ChunkSize  int         `json:"chunk_size"`
	TotalSize  int64       `json:"total_size"`
	ChunkCount int         `json:"chunk_count"`
	Chunks     []ChunkMeta `json:"chunks"`
	// Digest 为全部块内容拼接后的整体 SHA-256。
	Digest string `json:"digest"`
}

// Checkpoint 记录写入进度，用于中断恢复。
type Checkpoint struct {
	ObjectID     string `json:"object_id"`
	NextChunk    int    `json:"next_chunk"`
	BytesWritten int64  `json:"bytes_written"`
	// CRC 为上述字段的校验值，用于识别损坏的检查点。
	CRC uint32 `json:"crc"`
}
