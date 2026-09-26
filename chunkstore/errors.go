// Package chunkstore 提供面向本地文件系统的分块对象存储：
// 分块写入与条带分布、原子提交、并发提交仲裁、
// 写入检查点与中断恢复。读取路径对完整性做可区分校验。
package chunkstore

import "errors"

// 哨兵错误：调用方可用 errors.Is 区分失败原因。
var (
	// ErrObjectNotFound 表示对象不存在（无已提交的清单）。
	ErrObjectNotFound = errors.New("chunkstore: object not found")

	// ErrUploadNotFound 表示没有可恢复的上传会话（检查点不存在）。
	ErrUploadNotFound = errors.New("chunkstore: upload not found")

	// ErrCheckpointCorrupt 表示检查点损坏或不可信
	// （MAC 校验失败、内容自相矛盾或已落后于存储现状），
	// 该检查点必须被明确拒绝，不得从中间继续。
	ErrCheckpointCorrupt = errors.New("chunkstore: checkpoint corrupt or untrusted")

	// ErrChunkLengthMismatch 表示块长度与清单记录不一致。
	ErrChunkLengthMismatch = errors.New("chunkstore: chunk length mismatch")

	// ErrChunkChecksumMismatch 表示块内容校验失败。
	ErrChunkChecksumMismatch = errors.New("chunkstore: chunk checksum mismatch")

	// ErrChunkMissing 表示清单引用的块缺失。
	ErrChunkMissing = errors.New("chunkstore: chunk missing")

	// ErrManifestCorrupt 表示清单本身损坏或自相矛盾。
	ErrManifestCorrupt = errors.New("chunkstore: manifest corrupt")

	// ErrTooLarge 表示对象大小超过配置上限。
	ErrTooLarge = errors.New("chunkstore: object too large")

	// ErrCommitted 表示上传会话已提交，不能再写入。
	ErrCommitted = errors.New("chunkstore: upload already committed")

	// ErrAborted 表示上传会话已中止，不能再写入。
	ErrAborted = errors.New("chunkstore: upload aborted")
)

// ConflictError 表示并发提交冲突：同一对象已被其他写入方
// 以不同内容提交。判定规则为“先提交者获胜”，结果确定。
type ConflictError struct {
	Object         string
	ExistingDigest string
	AttemptDigest  string
}

func (e *ConflictError) Error() string {
	return "chunkstore: commit conflict on " + e.Object +
		" (existing=" + e.ExistingDigest + " attempt=" + e.AttemptDigest + ")"
}

// ChunkError 携带出错块的标识，便于定位与日志判定。
type ChunkError struct {
	Object string
	Index  int
	Hash   string
	Err    error
}

func (e *ChunkError) Error() string {
	return "chunkstore: object " + e.Object + " chunk " +
		itoa(e.Index) + " (" + e.Hash + "): " + e.Err.Error()
}

func (e *ChunkError) Unwrap() error { return e.Err }

// itoa 避免在骨架阶段引入 strconv 依赖；实现阶段可替换。
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
