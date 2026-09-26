package chunkstore

import (
	"io"
	"sync"
)

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

// CopyFrom 从 r 流式读入数据，分块持久化并推进检查点。
// 返回实际接收的字节数；内存占用与对象大小无关。
func (u *Upload) CopyFrom(r io.Reader) (int64, error) {
	return 0, nil // TODO: 实现
}

// Commit 提交对象：先校验期望总大小，再原子发布清单。
// 并发提交同一对象时“先提交者获胜”；相同内容的重复提交幂等成功，
// 不同内容的提交返回 *ConflictError。
func (u *Upload) Commit() (*Manifest, error) {
	return nil, nil // TODO: 实现
}

// Abort 中止上传并删除检查点；已写入的块保留供后续会话复用。
func (u *Upload) Abort() error {
	return nil // TODO: 实现
}
