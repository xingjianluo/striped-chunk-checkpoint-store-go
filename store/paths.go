package store

import (
	"path/filepath"
)

// casPath 返回内容块在 CAS 仓库中的路径：cas/sha256/<前2位>/<完整哈希>
func (s *Store) casPath(ref string) string {
	return filepath.Join(s.root, "cas", "sha256", ref[:2], ref)
}

// tombstonePath 返回对象墓碑文件路径。
func (s *Store) tombstonePath(id string) string {
	return filepath.Join(s.root, "tombstones", id+".json")
}

// gcDir / gcPlanPath / gcDoneDir 返回回收计划与逐块完成标记的位置。
func (s *Store) gcDir() string {
	return filepath.Join(s.root, "gc")
}

func (s *Store) gcPlanPath() string {
	return filepath.Join(s.gcDir(), "active.plan.json")
}

func (s *Store) gcDoneDir(planID string) string {
	return filepath.Join(s.gcDir(), planID+".done")
}

func (s *Store) gcDonePath(planID, ref string) string {
	return filepath.Join(s.gcDoneDir(planID), ref[:2], ref)
}
