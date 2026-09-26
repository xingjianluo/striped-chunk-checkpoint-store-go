package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// commitManifest 原子提交清单。
// 规则：先到先得——通过硬链接实现"不存在才创建"，
// 读取方只会看到完整清单，绝不观察到半提交状态。
// 已存在时：内容摘要一致则幂等成功，否则返回 ErrCommitConflict。
func (s *Store) commitManifest(m *Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	final := s.manifestPath(m.ObjectID)
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	defer os.Remove(tmp)

	err = os.Link(tmp, final)
	if err == nil {
		return syncDir(filepath.Dir(final))
	}
	if !errors.Is(err, fs.ErrExist) {
		return err
	}
	// 已存在：比较内容摘要，决定幂等成功或冲突。
	existing, lerr := s.loadManifest(m.ObjectID)
	if lerr != nil {
		return lerr
	}
	if existing.Digest == m.Digest && existing.TotalSize == m.TotalSize {
		return nil // 重复提交，幂等成功
	}
	return fmt.Errorf("%w: object %q already committed with different content",
		ErrCommitConflict, m.ObjectID)
}

// loadManifest 读取已提交清单；不存在返回 ErrObjectNotFound。
func (s *Store) loadManifest(id string) (*Manifest, error) {
	data, err := os.ReadFile(s.manifestPath(id))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%w: %q", ErrObjectNotFound, id)
		}
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: manifest of %q unreadable", ErrChecksumMismatch, id)
	}
	return &m, nil
}
