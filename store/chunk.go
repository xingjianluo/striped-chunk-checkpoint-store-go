package store

import (
	"fmt"
	"path/filepath"
)

// stripeFor 计算块所属的条带编号：按块序号轮转分布。
func (s *Store) stripeFor(index int) int {
	return index % s.stripes
}

// chunkPath 返回块文件在条带目录中的路径：
// <root>/data/stripe-<k>/<object-id>/chunk-<NNNNNN>
func (s *Store) chunkPath(id string, index int) string {
	return filepath.Join(s.root, "data",
		fmt.Sprintf("stripe-%d", s.stripeFor(index)),
		id,
		fmt.Sprintf("chunk-%06d", index))
}

// manifestPath 返回已提交清单的路径。
func (s *Store) manifestPath(id string) string {
	return filepath.Join(s.root, "objects", id+".json")
}

// checkpointPath 返回写入中检查点的路径。
func (s *Store) checkpointPath(id string) string {
	return filepath.Join(s.root, "staging", id+".checkpoint")
}

// tombstonePath 返回删除墓碑的路径。
func (s *Store) tombstonePath(id string) string {
	return filepath.Join(s.root, "objects", id+".tombstone")
}

// blobPath 返回内容寻址块的路径：
// <root>/data/blobs/<hh>/<sha256>，按哈希首字节分片。
func (s *Store) blobPath(hash string) string {
	return filepath.Join(s.root, "data", "blobs", hash[:2], hash)
}

// gcPlanPath 返回回收计划（回收日志）的路径。
func (s *Store) gcPlanPath() string {
	return filepath.Join(s.root, "gc", "plan.json")
}
