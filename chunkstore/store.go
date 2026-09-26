package chunkstore

import (
	"io"
	"sync"
)

// 目录布局：
//
//	<root>/config.json                 存储参数（块大小、条带数、密钥指纹）
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
func Open(root string, cfg Config, opts ...Option) (*Store, error) {
	return nil, nil // TODO: 实现
}

// Config 返回存储参数。
func (s *Store) Config() Config { return s.cfg }

// BeginUpload 开始一次新的上传会话。
// total 为期望的对象总大小，传负数表示未知（仅受 maxTotal 约束）。
func (s *Store) BeginUpload(object string, total int64) (*Upload, error) {
	return nil, nil // TODO: 实现
}

// ResumeUpload 依据检查点恢复上传会话。
// 检查点损坏或不可信时返回 ErrCheckpointCorrupt，绝不从中间继续。
func (s *Store) ResumeUpload(object string) (*Upload, error) {
	return nil, nil // TODO: 实现
}

// PutObject 以单调用方式写入并提交对象（内存占用有界）。
func (s *Store) PutObject(object string, r io.Reader) (*Manifest, error) {
	return nil, nil // TODO: 实现
}

// OpenObject 打开已提交对象进行流式读取。
// 打开时做完整性校验，失败原因可区分（见 errors.go）。
func (s *Store) OpenObject(object string) (*Reader, error) {
	return nil, nil // TODO: 实现
}

// VerifyObject 校验对象完整性而不读出数据。
func (s *Store) VerifyObject(object string) error {
	return nil // TODO: 实现
}

// ManifestOf 返回对象的已提交清单。
func (s *Store) ManifestOf(object string) (*Manifest, error) {
	return nil, nil // TODO: 实现
}
