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

// Tombstone 记录对象已被删除，使"已删除"与"从未写入"可区分。
type Tombstone struct {
	ObjectID  string `json:"object_id"`
	Digest    string `json:"digest"`
	TotalSize int64  `json:"total_size"`
	DeletedAt int64  `json:"deleted_at"`
	// CRC 为上述字段的校验值，用于识别损坏的墓碑。
	CRC uint32 `json:"crc"`
}

// GCPlan 描述一轮待回收的内容，落盘为回收日志。
// 回收被中断后可依据它继续；损坏的计划必须被拒绝。
type GCPlan struct {
	Version   int   `json:"version"`
	CreatedAt int64 `json:"created_at"`
	// Dirs 为待清理的对象块目录（对象 ID，来自被中断的删除）。
	Dirs []string `json:"dirs"`
	// Blobs 为待回收的内容寻址块（SHA-256 十六进制）。
	Blobs []string `json:"blobs"`
	// CRC 为上述字段的校验值。
	CRC uint32 `json:"crc"`
}

// GCStats 报告一轮回收的实际结果，重复执行不会重复计数。
type GCStats struct {
	// Resumed 为 true 表示本轮接续了上次未完成的回收计划。
	Resumed        bool
	DirsRemoved    int
	BlobsRemoved   int
	BytesReclaimed int64
}
