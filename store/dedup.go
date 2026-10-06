package store

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

// ensureBlob 把块内容写入内容寻址存储；相同内容的 blob 已存在时直接
// 复用（去重），跳过落盘。临时文件唯一命名 + rename，并发写同一内容
// 安全（后者原子覆盖，内容相同）。调用方需持有 gcMu 读锁。
func (s *Store) ensureBlob(hash string, data []byte) error {
	path := s.blobPath(hash)
	if _, err := os.Stat(path); err == nil {
		return nil // 去重命中：内容已存在，无需重复落盘
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	return syncDir(dir)
}

// linkChunk 把对象块路径硬链接到 blob：对象块文件成为 blob 的另一个
// 目录项，磁盘上相同内容只存一份。调用方需持有 gcMu 读锁。
func (s *Store) linkChunk(id string, index int, hash string) error {
	path := s.chunkPath(id, index)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	err := os.Link(s.blobPath(hash), path)
	if errors.Is(err, fs.ErrExist) {
		// 目标已存在（如重写残留）：替换后重试。
		if rerr := os.Remove(path); rerr != nil {
			return rerr
		}
		err = os.Link(s.blobPath(hash), path)
	}
	return err
}

// blobLinkCount 返回文件的硬链接数。blob 自身占 1；
// 大于 1 表示仍有对象块引用它，不得回收。
func blobLinkCount(path string) (uint64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, errors.New("store: cannot determine link count")
	}
	return uint64(st.Nlink), nil
}
