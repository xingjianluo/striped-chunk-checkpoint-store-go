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

// computeCRC 对墓碑关键字段计算校验值。
func (tb *Tombstone) computeCRC() uint32 {
	h := crc32.NewIEEE()
	fmt.Fprintf(h, "%s|%s|%d|%d", tb.ObjectID, tb.Digest, tb.TotalSize, tb.DeletedAt)
	return h.Sum32()
}

// Delete 删除已提交的对象。顺序：落墓碑 → 移除清单 → 移除块链接，
// 任何一步中断后重试都能完成剩余工作（幂等）。
//   - 删除已提交对象：成功返回 nil；
//   - 重复删除同一对象：幂等返回 nil（结果确定）；
//   - 删除从未写入的对象：返回 ErrObjectNotFound（与"已删除"可区分）；
//   - 墓碑损坏：返回 ErrTombstoneCorrupt，明确拒绝。
//
// 删除只移除本对象的块链接，内容寻址 blob 由其它对象的链接保护，
// 共享相同内容的对象不受影响；无引用的 blob 由 Collect 回收。
func (s *Store) Delete(id string) error {
	unlock := s.lockObject(id)
	defer unlock()

	m, merr := s.loadManifest(id)
	if merr != nil {
		if !errors.Is(merr, ErrObjectNotFound) {
			return merr
		}
		// 清单不存在：看墓碑区分"已删除"与"从未写入"。
		_, terr := s.loadTombstone(id)
		if terr == nil {
			// 已删除：完成上次可能中断的残留清理，幂等成功。
			return s.clearChunks(id)
		}
		if errors.Is(terr, fs.ErrNotExist) {
			return merr // 从未写入
		}
		return terr // 墓碑损坏：明确拒绝
	}

	// 清单存在：落墓碑（记录被删对象的摘要，便于审计与判定）。
	tb := &Tombstone{
		ObjectID:  id,
		Digest:    m.Digest,
		TotalSize: m.TotalSize,
		DeletedAt: time.Now().Unix(),
	}
	if err := s.saveTombstone(tb); err != nil {
		return err
	}
	if err := os.Remove(s.manifestPath(id)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := syncDir(filepath.Dir(s.manifestPath(id))); err != nil {
		return err
	}
	return s.clearChunks(id)
}

// loadTombstone 读取并校验墓碑。不存在返回 fs.ErrNotExist；
// 内容损坏或 CRC 不符返回 ErrTombstoneCorrupt。
func (s *Store) loadTombstone(id string) (*Tombstone, error) {
	data, err := os.ReadFile(s.tombstonePath(id))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fs.ErrNotExist
		}
		return nil, err
	}
	var tb Tombstone
	if err := json.Unmarshal(data, &tb); err != nil {
		return nil, fmt.Errorf("%w: unreadable tombstone for %q", ErrTombstoneCorrupt, id)
	}
	if tb.ObjectID != id || tb.DeletedAt < 0 {
		return nil, fmt.Errorf("%w: invalid fields for %q", ErrTombstoneCorrupt, id)
	}
	if tb.CRC != tb.computeCRC() {
		return nil, fmt.Errorf("%w: crc mismatch for %q", ErrTombstoneCorrupt, id)
	}
	return &tb, nil
}

// saveTombstone 原子写入墓碑：临时文件 + rename + 目录同步。
func (s *Store) saveTombstone(tb *Tombstone) error {
	tb.CRC = tb.computeCRC()
	data, err := json.Marshal(tb)
	if err != nil {
		return err
	}
	path := s.tombstonePath(tb.ObjectID)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// removeTombstone 在新提交成功后移除墓碑（对象由"已删除"回到"已提交"）。
func (s *Store) removeTombstone(id string) error {
	err := os.Remove(s.tombstonePath(id))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// deletedState 在清单缺失时判定对象状态：
// 已删除返回 ErrObjectDeleted，墓碑损坏返回 ErrTombstoneCorrupt，
// 从未写入返回 nil（调用方继续返回 ErrObjectNotFound）。
func (s *Store) deletedState(id string) error {
	_, err := s.loadTombstone(id)
	switch {
	case err == nil:
		return fmt.Errorf("%w: %q", ErrObjectDeleted, id)
	case errors.Is(err, fs.ErrNotExist):
		return nil
	default:
		return err
	}
}
