package chunkstore

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

// 目录布局：
//
//	<root>/config.json                 存储参数（块大小、条带数）
//	<root>/secret                      检查点签名密钥（仅本机，0600）
//	<root>/stripes/<k>/chunks/<hash>   条带 k 上的内容寻址块
//	<root>/objects/<name>/manifest.json    已提交清单（原子替换）
//	<root>/objects/<name>/commit.lock      提交仲裁锁
//	<root>/checkpoints/<uploadID>.json     上传会话检查点
const (
	configFileName   = "config.json"
	secretFileName   = "secret"
	stripesDirName   = "stripes"
	chunksDirName    = "chunks"
	objectsDirName   = "objects"
	manifestFileName = "manifest.json"
	commitLockName   = "commit.lock"
	checkpointsDir   = "checkpoints"

	checkpointVersion = 1
	secretLen         = 32
)

// Config 是存储的固定参数，创建后不可变更。
type Config struct {
	ChunkSize int64 `json:"chunk_size"` // 每块字节数，>0
	Stripes   int   `json:"stripes"`    // 条带目录数量，>0
}

// Option 调整 Store 行为。
type Option func(*Store)

// WithLogger 设置判定依据日志输出。
func WithLogger(l Logger) Option {
	return func(s *Store) { s.log = l }
}

// Logger 是可选的日志接口，测试用它打印块标识、
// 检查点位置与判定依据。
type Logger interface {
	Printf(format string, args ...any)
}

// Store 是本地文件系统上的分块对象存储。
type Store struct {
	root      string
	cfg       Config
	secret    []byte
	log       Logger
	mu        sync.Mutex
	openLocks map[string]*sync.Mutex // 进程内每对象提交互斥
}

// Open 打开或创建位于 root 的存储。
// 已存在的存储参数必须与 cfg 一致，否则报错。
func Open(root string, cfg Config, opts ...Option) (*Store, error) {
	if cfg.ChunkSize <= 0 {
		return nil, fmt.Errorf("chunkstore: chunk size must be positive, got %d", cfg.ChunkSize)
	}
	if cfg.Stripes <= 0 {
		return nil, fmt.Errorf("chunkstore: stripes must be positive, got %d", cfg.Stripes)
	}
	s := &Store{root: root, cfg: cfg, openLocks: make(map[string]*sync.Mutex)}
	for _, o := range opts {
		o(s)
	}
	if s.log == nil {
		s.log = discardLogger{}
	}
	if err := s.initLayout(); err != nil {
		return nil, err
	}
	if err := s.initConfig(); err != nil {
		return nil, err
	}
	if err := s.initSecret(); err != nil {
		return nil, err
	}
	return s, nil
}

// Config 返回存储参数。
func (s *Store) Config() Config { return s.cfg }

func (s *Store) initLayout() error {
	dirs := []string{
		filepath.Join(s.root, checkpointsDir),
		filepath.Join(s.root, objectsDirName),
	}
	for k := 0; k < s.cfg.Stripes; k++ {
		dirs = append(dirs, s.stripeDir(k))
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) initConfig() error {
	path := filepath.Join(s.root, configFileName)
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		blob, err := json.Marshal(s.cfg)
		if err != nil {
			return err
		}
		if err := os.WriteFile(path, blob, 0o644); err != nil {
			return err
		}
		return syncDir(s.root)
	}
	if err != nil {
		return err
	}
	var existing Config
	if err := json.Unmarshal(data, &existing); err != nil {
		return fmt.Errorf("chunkstore: unreadable config: %w", err)
	}
	if existing != s.cfg {
		return fmt.Errorf("chunkstore: config mismatch: stored %+v, requested %+v", existing, s.cfg)
	}
	return nil
}

func (s *Store) initSecret() error {
	path := filepath.Join(s.root, secretFileName)
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		secret := make([]byte, secretLen)
		if _, err := rand.Read(secret); err != nil {
			return err
		}
		if err := os.WriteFile(path, secret, 0o600); err != nil {
			return err
		}
		if err := syncDir(s.root); err != nil {
			return err
		}
		s.secret = secret
		return nil
	}
	if err != nil {
		return err
	}
	if len(data) != secretLen {
		return fmt.Errorf("chunkstore: secret file has unexpected length %d", len(data))
	}
	s.secret = data
	return nil
}

// ---- 路径与命名 ----

func (s *Store) stripeDir(k int) string {
	return filepath.Join(s.root, stripesDirName, itoa(k), chunksDirName)
}

// chunkPath 返回内容寻址块的完整路径。
func (s *Store) chunkPath(stripe int, hash string) string {
	return filepath.Join(s.stripeDir(stripe), hash)
}

func validateObjectName(name string) error {
	if name == "" || name == "." || name == ".." ||
		strings.ContainsAny(name, "/\\\x00") ||
		strings.HasPrefix(name, ".") {
		return fmt.Errorf("chunkstore: invalid object name %q", name)
	}
	return nil
}

func (s *Store) objectDir(object string) string {
	return filepath.Join(s.root, objectsDirName, object)
}

func (s *Store) manifestPath(object string) string {
	return filepath.Join(s.objectDir(object), manifestFileName)
}

// ---- 块读写 ----

// writeChunk 将一块数据写入条带目录（内容寻址）。
// 相同内容的块已存在时跳过写入（幂等去重）；
// 同名但内容不符视为损坏，重写修正。返回是否真正写盘。
func (s *Store) writeChunk(stripe int, hash string, data []byte) (bool, error) {
	path := s.chunkPath(stripe, hash)
	if fi, err := os.Stat(path); err == nil && fi.Size() == int64(len(data)) {
		if ok, _ := s.chunkIntact(&ChunkRef{Stripe: stripe, Size: int64(len(data)), Hash: hash}); ok {
			return false, nil // 已存在且完整：幂等跳过
		}
	}
	tmp := path + ".tmp." + randSuffix()
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return false, err
	}
	if err := syncFile(tmp); err != nil {
		return false, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return false, err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return false, err
	}
	return true, nil
}

// chunkIntact 校验磁盘上的块与记录一致（存在、长度、内容哈希）。
func (s *Store) chunkIntact(c *ChunkRef) (bool, error) {
	f, err := os.Open(s.chunkPath(c.Stripe, c.Hash))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.CopyBuffer(h, f, make([]byte, 32*1024))
	if err != nil {
		return false, err
	}
	if n != c.Size {
		return false, nil
	}
	return hex.EncodeToString(h.Sum(nil)) == c.Hash, nil
}

// ---- 清单读写 ----

// readManifest 读取已提交清单；不存在返回 ErrObjectNotFound。
func (s *Store) readManifest(object string) (*Manifest, error) {
	data, err := os.ReadFile(s.manifestPath(object))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrObjectNotFound
		}
		return nil, err
	}
	m, err := decodeManifest(data)
	if err != nil {
		return nil, err
	}
	if err := m.validate(); err != nil {
		return nil, err
	}
	return m, nil
}

// writeManifestAtomic 原子发布清单（tmp + rename + 目录 fsync）。
// 调用方必须持有该对象的提交锁。
func (s *Store) writeManifestAtomic(m *Manifest) error {
	dir := s.objectDir(m.Object)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	blob, err := encodeManifest(m)
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, manifestFileName+".tmp")
	if err := os.WriteFile(tmp, blob, 0o644); err != nil {
		return err
	}
	if err := syncFile(tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.manifestPath(m.Object)); err != nil {
		return err
	}
	return syncDir(dir)
}

// ---- 提交仲裁锁 ----

// objectLock 返回进程内每对象互斥锁。
func (s *Store) objectLock(object string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.openLocks[object]
	if !ok {
		l = &sync.Mutex{}
		s.openLocks[object] = l
	}
	return l
}

// withCommitLock 在进程内互斥锁与跨进程文件锁双重保护下执行 fn。
func (s *Store) withCommitLock(object string, fn func() error) error {
	l := s.objectLock(object)
	l.Lock()
	defer l.Unlock()

	dir := s.objectDir(object)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	lockPath := filepath.Join(dir, commitLockName)
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return fn()
}

// ---- 持久化辅助 ----

func syncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func randSuffix() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

// ---- 上传会话指针 ----
//
// 每个对象的每个活动会话在 checkpoints/by-object/<object>/ 下
// 有一个以 uploadID 命名的标记文件。标记只用于发现会话；
// 恢复时检查点内容本身必须通过 MAC 与结构校验。

func (s *Store) pointerDir(object string) string {
	return filepath.Join(s.root, checkpointsDir, "by-object", object)
}

func (s *Store) setUploadPointer(object, uploadID string) error {
	dir := s.pointerDir(object)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, uploadID)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return syncDir(dir)
}

func (s *Store) clearUploadPointer(object, uploadID string) {
	dir := s.pointerDir(object)
	_ = os.Remove(filepath.Join(dir, uploadID))
	_ = syncDir(dir)
}

// uploadIDFor 返回对象最近创建的活动会话 ID。
func (s *Store) uploadIDFor(object string) (string, error) {
	dir := s.pointerDir(object)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", ErrUploadNotFound
		}
		return "", err
	}
	var latest string
	var latestTime os.FileInfo
	for _, e := range entries {
		fi, err := e.Info()
		if err != nil {
			continue
		}
		if latestTime == nil || fi.ModTime().After(latestTime.ModTime()) {
			latest, latestTime = e.Name(), fi
		}
	}
	if latest == "" {
		return "", ErrUploadNotFound
	}
	return latest, nil
}
