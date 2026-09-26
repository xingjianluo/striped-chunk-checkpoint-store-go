package chunkstore

import "io"

// Reader 按序读取已提交对象的内容。
// 打开时已通过完整性校验，读取过程内存占用有界。
type Reader struct {
	manifest *Manifest
	store    *Store
	pos      int64
	chunkIdx int
	cur      io.ReadCloser
	curOff   int64
}

// Read 实现 io.Reader。
func (r *Reader) Read(p []byte) (int, error) {
	return 0, io.EOF // TODO: 实现
}

// Close 释放读取资源。
func (r *Reader) Close() error {
	return nil // TODO: 实现
}

// Size 返回对象总大小。
func (r *Reader) Size() int64 { return r.manifest.TotalSize }

// Manifest 返回对象清单。
func (r *Reader) Manifest() *Manifest { return r.manifest }
