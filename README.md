# striped-chunk-checkpoint-store-go

面向本地文件系统的分块对象存储组件：用本地目录模拟块设备与写入检查点，
支持并发提交、写入中断与恢复，数据状态可预期、可验证。
写入与读取的内存占用均有界（不超过一个块加固定缓冲），与对象大小无关。

## 目录布局

```
<root>/config.json                  存储参数（块大小、条带数），创建后不可变更
<root>/secret                       检查点 HMAC 签名密钥（0600，仅本机）
<root>/stripes/<k>/chunks/<hash>    条带 k 上的内容寻址块（SHA-256 命名）
<root>/objects/<name>/manifest.json     已提交清单（tmp+rename 原子发布）
<root>/objects/<name>/commit.lock       提交仲裁锁（flock）
<root>/checkpoints/<uploadID>.json      上传会话检查点（HMAC 签名）
<root>/checkpoints/by-object/<name>/    对象 → 会话的发现标记
```

## 分块与提交规则

- **分块**：对象按固定 `ChunkSize` 顺序切块，最后一块可短；
  块 `i` 写入条带目录 `i % Stripes`，以内容 SHA-256 命名（内容寻址）。
- **去重与幂等**：相同内容的块已存在且完整时跳过写入；
  同名但内容不符视为损坏并重写修正。
- **对象摘要**：链式摘要 `d0=sha256("chunkstore-v1"‖0x00‖h0)`、
  `d_i=sha256(d_{i-1}‖0x00‖h_i)`，空对象为 `sha256("chunkstore-v1")`。
  该方案无需序列化哈希内部状态即可从恢复点继续计算。
- **原子提交**：清单经 `tmp + rename + fsync` 原子发布，
  读取方只会看到完整清单或看不到，绝不观察半提交状态。
- **并发仲裁**：同一对象的提交在「进程内互斥锁 + flock 文件锁」下串行化，
  规则为**先提交者获胜**：
  - 无人提交过 → 发布，获胜；
  - 已存在且摘要相同 → 幂等成功，返回既有清单；
  - 已存在且摘要不同 → 返回 `*ConflictError`（携带双方摘要），结果确定可解释。
- **读取校验**：`OpenObject`/`VerifyObject` 逐块核对，失败原因可区分：
  `ErrChunkMissing`（块缺失）、`ErrChunkLengthMismatch`（长度不符）、
  `ErrChunkChecksumMismatch`（内容校验失败），统一包装为 `*ChunkError`
  （携带对象名、块下标与块哈希）；清单自身损坏报 `ErrManifestCorrupt`。

## 恢复语义

- **先数据后检查点**：每个块持久化（含目录 fsync）后才推进检查点，
  因此恢复位置**永不回退**，已完成的数据不会重复写入或丢失；
  崩溃时最多丢失不足一块的内存缓冲。
- **恢复流程**：`ResumeUpload(object)` 读取检查点 → 校验 HMAC →
  校验结构自洽（版本、存储参数、下标/偏移/摘要一致性）→
  逐块核对磁盘上的块真实存在且内容一致 → 从 `NextOffset` 继续写入。
- **明确拒绝**：MAC 失配、结构矛盾、与磁盘现状不符的检查点一律返回
  `ErrCheckpointCorrupt`，**绝不从中间继续**；无检查点返回 `ErrUploadNotFound`。
- **提交即清理**：`Commit` 成功后删除检查点与发现标记；
  `Abort` 删除检查点但保留已写入的块供后续会话去重复用。

## 验证方法

```bash
go test -race -v ./...
```

测试覆盖：

| 测试 | 验证点 |
| --- | --- |
| `TestBasicRoundTrip` / `TestEmptyObject` | 分块、条带分布、读写一致 |
| `TestChunkFailureReasons` | 块缺失/长度不符/校验失败三类原因可区分 |
| `TestCheckpointResumeAfterCrash` | 中断后重启恢复，位置不回退，内容一致 |
| `TestCheckpointTamperedRejected` | 篡改/不可信检查点被明确拒绝 |
| `TestCheckpointMonotonic` | 多次中断期间检查点位置单调不减 |
| `TestConcurrentCommitSameContent` | 并发同内容提交幂等，digest 一致 |
| `TestConcurrentCommitConflict` | 并发异内容提交先提交者获胜，读不到半提交状态 |
| `TestDuplicateCommitIdempotent` | 重复提交幂等 |

单测日志会打印块标识（index/stripe/hash）、检查点位置（index/offset）
与判定依据（如 `verdict=published / idempotent-replay / conflict`、
`verdict=missing / length-mismatch / checksum-mismatch`）。
