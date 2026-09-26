package chunkstore

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// Checkpoint 记录一次上传会话的可恢复进度。
// 检查点只在对应块持久化之后落盘（先写数据，后推进检查点），
// 因此恢复位置永不回退、已完成数据不会丢失。
type Checkpoint struct {
	Version    int        `json:"version"`
	UploadID   string     `json:"upload_id"`
	Object     string     `json:"object"`
	ChunkSize  int64      `json:"chunk_size"`
	Stripes    int        `json:"stripes"`
	NextIndex  int        `json:"next_index"`  // 下一个待写块下标
	NextOffset int64      `json:"next_offset"` // 已持久化的字节数
	TotalLimit int64      `json:"total_limit"` // 期望总大小，<0 表示未知
	Digest     string     `json:"digest"`      // 已持久化部分的链式摘要
	Chunks     []ChunkRef `json:"chunks"`
}

// checkpointEnvelope 是检查点文件的磁盘格式：
// 内容 + HMAC-SHA256 签名。签名不匹配即视为不可信。
type checkpointEnvelope struct {
	Data json.RawMessage `json:"data"`
	MAC  string          `json:"mac"`
}

// macOf 计算检查点内容的 HMAC-SHA256。
func macOf(secret, data []byte) string {
	h := hmac.New(sha256.New, secret)
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

// checkpointPath 返回检查点文件路径。
func (s *Store) checkpointPath(uploadID string) string {
	return filepath.Join(s.root, checkpointsDir, uploadID+".json")
}

// writeCheckpoint 原子落盘检查点（tmp + rename + 目录 fsync）。
func (s *Store) writeCheckpoint(cp *Checkpoint) error {
	data, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	env := checkpointEnvelope{Data: data, MAC: macOf(s.secret, data)}
	blob, err := json.Marshal(env)
	if err != nil {
		return err
	}
	path := s.checkpointPath(cp.UploadID)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, blob, 0o600); err != nil {
		return err
	}
	if err := syncFile(tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// readCheckpoint 读取并校验检查点。
// MAC 不匹配、结构非法或版本不符一律返回 ErrCheckpointCorrupt。
func (s *Store) readCheckpoint(uploadID string) (*Checkpoint, error) {
	path := s.checkpointPath(uploadID)
	blob, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrUploadNotFound
		}
		return nil, err
	}
	var env checkpointEnvelope
	if err := json.Unmarshal(blob, &env); err != nil {
		return nil, fmt.Errorf("%w: unreadable envelope", ErrCheckpointCorrupt)
	}
	if !hmac.Equal([]byte(env.MAC), []byte(macOf(s.secret, env.Data))) {
		return nil, fmt.Errorf("%w: MAC mismatch", ErrCheckpointCorrupt)
	}
	var cp Checkpoint
	if err := json.Unmarshal(env.Data, &cp); err != nil {
		return nil, fmt.Errorf("%w: unreadable payload", ErrCheckpointCorrupt)
	}
	if err := s.validateCheckpoint(&cp); err != nil {
		return nil, err
	}
	return &cp, nil
}

// validateCheckpoint 校验检查点结构自洽且与存储参数一致。
func (s *Store) validateCheckpoint(cp *Checkpoint) error {
	if cp.Version != checkpointVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrCheckpointCorrupt, cp.Version)
	}
	if cp.UploadID == "" || cp.Object == "" {
		return fmt.Errorf("%w: empty upload id or object", ErrCheckpointCorrupt)
	}
	if cp.ChunkSize != s.cfg.ChunkSize || cp.Stripes != s.cfg.Stripes {
		return fmt.Errorf("%w: store config mismatch (chunk_size=%d stripes=%d)",
			ErrCheckpointCorrupt, cp.ChunkSize, cp.Stripes)
	}
	if cp.NextIndex != len(cp.Chunks) {
		return fmt.Errorf("%w: next_index=%d but %d chunk records",
			ErrCheckpointCorrupt, cp.NextIndex, len(cp.Chunks))
	}
	var off int64
	for i, c := range cp.Chunks {
		if c.Index != i || c.Size <= 0 || c.Size > cp.ChunkSize {
			return fmt.Errorf("%w: chunk record %d inconsistent", ErrCheckpointCorrupt, i)
		}
		if c.Stripe != i%s.cfg.Stripes {
			return fmt.Errorf("%w: chunk %d stripe mismatch", ErrCheckpointCorrupt, i)
		}
		off += c.Size
	}
	if off != cp.NextOffset {
		return fmt.Errorf("%w: next_offset=%d but chunks sum to %d",
			ErrCheckpointCorrupt, cp.NextOffset, off)
	}
	if cp.Digest != chainDigest(chunkHashes(cp.Chunks)) {
		return fmt.Errorf("%w: digest does not match chunk list", ErrCheckpointCorrupt)
	}
	return nil
}

// removeCheckpoint 删除检查点（提交或中止后调用）。
func (s *Store) removeCheckpoint(uploadID string) error {
	err := os.Remove(s.checkpointPath(uploadID))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return syncDir(filepath.Join(s.root, checkpointsDir))
}

// verifyCheckpointChunks 核对检查点引用的块在磁盘上真实存在且内容一致。
// 这是“不可信检查点不得从中间继续”的第二道防线：
// 即使 MAC 正确，块缺失或被改写也拒绝恢复。
func (s *Store) verifyCheckpointChunks(cp *Checkpoint) error {
	for _, c := range cp.Chunks {
		ok, err := s.chunkIntact(&c)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w: chunk %d (%s) missing or altered on disk",
				ErrCheckpointCorrupt, c.Index, c.Hash)
		}
	}
	return nil
}
