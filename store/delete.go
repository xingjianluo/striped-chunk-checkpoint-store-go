package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// computeCRC 覆盖墓碑关键字段。
func (t *Tombstone) computeCRC() uint32 {
	h := crc32.NewIEEE()
	fmt.Fprintf(h, "%s|%s|%d", t.ObjectID, t.Digest, t.DeletedAtUnix)
	return h.Sum32()
}

// commitTombstone 原子落盘墓碑（临时文件 + rename + 目录 fsync）。
func (s *Store) commitTombstone(t *Tombstone) error {
	t.CRC = t.computeCRC()
	data, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return err
	}
	final := s.tombstonePath(t.ObjectID)
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return err
	}
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	return syncDir(filepath.Dir(final))
}

// loadTombstone 读取墓碑。
// 文件不存在返回 fs.ErrNotExist；墓碑损坏（无法解析 / CRC 不符）时
// 保守地按"已删除"处理并返回包装了 ErrObjectDeleted 的错误——
// 因为清单已不存在，无论如何都不能再把内容当作完整对象读出。
func (s *Store) loadTombstone(id string) (*Tombstone, error) {
	data, err := os.ReadFile(s.tombstonePath(id))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fs.ErrNotExist
		}
		return nil, err
	}
	var t Tombstone
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("%w: tombstone of %q unreadable", ErrObjectDeleted, id)
	}
	if t.ObjectID != id || t.CRC != t.computeCRC() {
		return nil, fmt.Errorf("%w: tombstone of %q crc mismatch", ErrObjectDeleted, id)
	}
	return &t, nil
}

// Delete 删除一个已提交对象。
//
// 顺序经过设计以保证崩溃与并发下的确定性：
//  1. 对象锁序列化同一 id 的并发操作；
//  2. 已存在墓碑 -> 幂等成功；清单与墓碑都不存在 -> ErrObjectNotFound
//     （"已删除"与"从未写过"严格区分）；
//  3. 在生命周期写锁下先原子落墓碑、再移除清单：写锁保证没有正在进行
//     的读取或提交依赖该对象，落墓碑后即使立刻崩溃，结论也已确定；
//  4. v1（无 CAS 引用）对象的私有块由对象独占，直接删除；
//     v2 对象的内容块可能正被其它对象共享，留给 Reclaim 按引用回收。
func (s *Store) Delete(id string) error {
	unlock := s.lockObject(id)
	defer unlock()

	if _, err := s.loadTombstone(id); err == nil {
		return nil // 重复删除：幂等
	} else if !errors.Is(err, fs.ErrNotExist) {
		if errors.Is(err, ErrObjectDeleted) {
			return nil // 墓碑损坏：保守幂等
		}
		return err
	}

	m, err := s.loadManifest(id)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			return fmt.Errorf("%w: %q", ErrObjectNotFound, id)
		}
		return err
	}

	s.life.Lock()
	defer s.life.Unlock()

	t := &Tombstone{ObjectID: id, Digest: m.Digest, DeletedAtUnix: time.Now().Unix()}
	if err := s.commitTombstone(t); err != nil {
		return err
	}
	if err := os.Remove(s.manifestPath(id)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := syncDir(filepath.Dir(s.manifestPath(id))); err != nil {
		return err
	}
	// 清理写入暂存与对象私有块；v1 对象的实际数据在此真正释放，
	// v2 对象的共享内容由 Reclaim 依据全局引用关系释放。
	if err := s.removeCheckpoint(id); err != nil {
		return err
	}
	return s.clearChunks(id)
}

// chunkPathFor 返回某个块当前应读取的物理路径：
// 带 Ref 的 v2 块从 CAS 读取（可能被多对象共享），
// 无 Ref 的 v1 块从对象私有条带目录读取。
func (s *Store) chunkPathFor(m *Manifest, c ChunkMeta) string {
	if c.Ref != "" {
		return s.casPath(c.Ref)
	}
	return s.chunkPath(m.ObjectID, c.Index)
}
