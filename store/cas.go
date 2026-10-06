package store

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// publishChunks 将提交窗口内的暂存块提升到内容寻址仓库（CAS）。
// 每个块以其内容 SHA-256 命名：相同内容在磁盘上只保留一份物理文件，
// 多个对象（及同一对象的多版本）通过硬链接共享。已存在且长度一致的
// 内容块直接复用，不重写；长度不符意味着内容哈希碰撞或外部损坏，
// 明确报错而不是覆盖。
//
// 调用方必须持有 life 读锁（提交窗口），保证 Reclaim 不会在提升期间
// 把刚出现的块当作无引用块删除。
func (s *Store) publishChunks(m *Manifest) error {
	for i := range m.Chunks {
		c := &m.Chunks[i]
		if c.Ref == "" {
			c.Ref = c.SHA256
		}
		if len(c.Ref) != 64 {
			return fmt.Errorf("%w: invalid content ref %q for chunk %d",
				ErrChecksumMismatch, c.Ref, c.Index)
		}
		staging := s.chunkPath(m.ObjectID, c.Index)
		target := s.casPath(c.Ref)
		if s.casBlobExists(c.Ref, c.Length) {
			continue // 内容已存在，物理上共享同一份
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		// 先链接到临时名再 rename，避免与并发提交/回收观察到半成品。
		tmp := target + ".link.tmp"
		_ = os.Remove(tmp)
		if err := os.Link(staging, tmp); err != nil {
			return fmt.Errorf("publish chunk %d of %q: %w", c.Index, m.ObjectID, err)
		}
		if err := os.Rename(tmp, target); err != nil {
			_ = os.Remove(tmp)
			if !errors.Is(err, fs.ErrExist) {
				return err
			}
		}
		if err := syncDir(filepath.Dir(target)); err != nil {
			return err
		}
	}
	m.Version = 2
	return nil
}

// casBlobExists 报告指定 ref 的内容块是否已存在且长度符合预期。
func (s *Store) casBlobExists(ref string, want int64) bool {
	fi, err := os.Stat(s.casPath(ref))
	if err != nil {
		return false
	}
	return fi.Size() == want
}

// listCASBlobs 遍历 CAS 仓库，返回 ref -> 字节长度。
// 忽略非哈希命名的临时文件（.link.tmp 等）。
func (s *Store) listCASBlobs() (map[string]int64, error) {
	out := map[string]int64{}
	root := filepath.Join(s.root, "cas", "sha256")
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) && path == root {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if len(name) != 64 { // 跳过临时文件 / 非内容文件
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		out[name] = fi.Size()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
