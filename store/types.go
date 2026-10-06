package store

// ChunkMeta 描述单个块在清单中的记录。
type ChunkMeta struct {
	Index  int    `json:"index"`
	Stripe int    `json:"stripe"`
	Length int64  `json:"length"`
	SHA256 string `json:"sha256"`
	// Ref 为块内容在内容寻址仓库（CAS）中的键（块内容的 SHA-256 十六进制）。
	// 非空时块数据从 cas 目录读取，多个对象的相同块共享同一份文件；
	// 为空时按 v1 布局从对象私有条带目录读取（老数据兼容）。
	Ref string `json:"ref,omitempty"`
}

// Manifest 是对象提交后的不可变元数据。
type Manifest struct {
	// Version 为清单布局版本：1（或零值）= 对象私有块；2 = CAS 去重块。
	Version    int         `json:"version,omitempty"`
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

// Tombstone 是对象被删除后的墓碑记录。
// 存在墓碑即代表"对象曾经提交且已被删除"，与"从未写入"严格区分。
type Tombstone struct {
	ObjectID string `json:"object_id"`
	// Digest 为被删对象提交时的整体摘要，便于审计，不参与读取判定。
	Digest string `json:"digest"`
	// DeletedAtUnix 为删除时间（Unix 秒）。
	DeletedAtUnix int64 `json:"deleted_at_unix"`
	// CRC 覆盖上述字段，损坏的墓碑按"已删除"保守处理但会记录告警。
	CRC uint32 `json:"crc"`
}

// gcPlanEntry 描述回收计划中的一个候选内容块。
type gcPlanEntry struct {
	Ref    string `json:"ref"`
	Length int64  `json:"length"`
}

// ReclaimPlan 是一次空间回收的待办清单（标记阶段产物）。
// 文件带整体 CRC；任何篡改或字段不一致都导致恢复时明确拒绝执行。
type ReclaimPlan struct {
	// ID 为本次回收的唯一标识（时间戳），对应 gc/<ID>.plan.json。
	ID string `json:"id"`
	// Entries 为经判定"无任何存活对象引用"的候选块。
	Entries []gcPlanEntry `json:"entries"`
	// TotalBytes 为 Entries 长度之和，删除后用于核对实际回收量。
	TotalBytes int64 `json:"total_bytes"`
	// CRC 覆盖 ID 与全部 Entries，防止计划被篡改后误删数据。
	CRC uint32 `json:"crc"`
}

// ReclaimReport 汇报一次回收（含中断后续跑）的结果。
type ReclaimReport struct {
	// PlanID 为执行的回收计划标识。
	PlanID string
	// DeletedBlocks / ReclaimedBytes 为本次调用实际删除的块数与字节数
	// （续跑时只计入此前未完成的部分，不重复计数）。
	DeletedBlocks  int
	ReclaimedBytes int64
	// SkippedBlocks 为计划中因重新获得引用而跳过、未删除的块数。
	SkippedBlocks int
	// Resumed 表示本次执行是否接续了一个之前中断的计划。
	Resumed bool
}
