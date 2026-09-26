package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
)

// computeCRC 对检查点关键字段计算校验值。
func (cp *Checkpoint) computeCRC() uint32 {
	h := crc32.NewIEEE()
	fmt.Fprintf(h, "%s|%d|%d", cp.ObjectID, cp.NextChunk, cp.BytesWritten)
	return h.Sum32()
}

// saveCheckpoint 原子写入检查点：先写临时文件再 rename，
// 并同步目录，保证崩溃后要么看到旧值要么看到新值。
func (s *Store) saveCheckpoint(cp *Checkpoint) error {
	cp.CRC = cp.computeCRC()
	data, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	path := s.checkpointPath(cp.ObjectID)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// loadCheckpoint 读取并校验检查点。
// 不存在返回 fs.ErrNotExist；内容损坏或 CRC 不符返回 ErrCheckpointCorrupt，
// 调用方必须拒绝恢复而不是从中间继续。
func (s *Store) loadCheckpoint(id string) (*Checkpoint, error) {
	data, err := os.ReadFile(s.checkpointPath(id))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fs.ErrNotExist
		}
		return nil, err
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("%w: unreadable checkpoint for %q", ErrCheckpointCorrupt, id)
	}
	if cp.ObjectID != id || cp.NextChunk < 0 || cp.BytesWritten < 0 {
		return nil, fmt.Errorf("%w: invalid fields for %q", ErrCheckpointCorrupt, id)
	}
	if cp.CRC != cp.computeCRC() {
		return nil, fmt.Errorf("%w: crc mismatch for %q", ErrCheckpointCorrupt, id)
	}
	return &cp, nil
}

// removeCheckpoint 在提交成功后删除检查点。
func (s *Store) removeCheckpoint(id string) error {
	err := os.Remove(s.checkpointPath(id))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// syncDir fsync 目录，确保目录项落盘。
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
