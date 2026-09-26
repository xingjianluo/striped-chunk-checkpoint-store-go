package chunkstore

import "encoding/json"

// Checkpoint 记录一次上传会话的可恢复进度。
// 检查点只在对应块持久化之后落盘（先写数据，后推进检查点），
// 因此恢复位置永不回退、已完成数据不会丢失。
type Checkpoint struct {
	Version    int        `json:"version"`
	UploadID   string     `json:"upload_id"`
	Object     string     `json:"object"`
	ChunkSize  int64      `json:"chunk_size"`
	Stripes    int        `json:"stripes"`
	NextIndex  int        `json:"next_index"`  // 下一个待写块下标
	NextOffset int64      `json:"next_offset"` // 已持久化的字节数
	TotalLimit int64      `json:"total_limit"` // 期望总大小上限，<0 表示未知
	Digest     string     `json:"digest"`      // 已持久化部分的链式摘要
	Chunks     []ChunkRef `json:"chunks"`
}

// checkpointEnvelope 是检查点文件的磁盘格式：
// 内容 + HMAC-SHA256 签名。签名不匹配即视为不可信。
type checkpointEnvelope struct {
	Data json.RawMessage `json:"data"`
	MAC  string          `json:"mac"`
}
