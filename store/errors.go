package store

import "errors"

// 可区分的失败原因，均可用 errors.Is 判定。
var (
	// ErrObjectNotFound 对象从未提交或清单不存在。
	ErrObjectNotFound = errors.New("store: object not found")
	// ErrChunkMissing 清单引用的块文件在磁盘上缺失。
	ErrChunkMissing = errors.New("store: chunk missing")
	// ErrChunkLengthMismatch 块实际长度与清单记录不一致。
	ErrChunkLengthMismatch = errors.New("store: chunk length mismatch")
	// ErrChecksumMismatch 块或整体内容校验失败。
	ErrChecksumMismatch = errors.New("store: checksum mismatch")
	// ErrCommitConflict 同一对象已被提交且内容不同。
	ErrCommitConflict = errors.New("store: commit conflict with existing object")
	// ErrCheckpointCorrupt 检查点损坏或不可信，拒绝从中间继续。
	ErrCheckpointCorrupt = errors.New("store: checkpoint corrupt")
	// ErrCheckpointRegression 恢复位置回退，被拒绝。
	ErrCheckpointRegression = errors.New("store: checkpoint regression")
	// ErrObjectDeleted 对象曾提交但已被删除（与从未写入可区分）。
	ErrObjectDeleted = errors.New("store: object deleted")
	// ErrTombstoneCorrupt 删除墓碑损坏或不可信，无法判定对象状态。
	ErrTombstoneCorrupt = errors.New("store: tombstone corrupt")
	// ErrGCPlanCorrupt 待回收记录损坏或不可信，拒绝按它删除数据。
	ErrGCPlanCorrupt = errors.New("store: gc plan corrupt")
)
