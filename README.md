# striped-chunk-checkpoint-store-go

基于本地文件系统的分块条带存储与并发写入组件。用本地目录模拟块设备与写入检查点，
使并发提交、写入中断与恢复后的数据状态可预期且可验证。写入与读取的内存占用恒定
（单块缓冲区），不随对象大小增长。

## 目录布局

```
<root>/
  data/stripe-<k>/<object-id>/chunk-NNNNNN   # 块文件，按 块序号 % 条带数 轮转分布
  objects/<object-id>.json                   # 已提交清单（不可变元数据）
  staging/<object-id>.checkpoint             # 写入中检查点
```

## 分块与提交规则

- 对象被切分为定长块（默认 64KiB，可通过 `Options.ChunkSize` 调整），
  第 `i` 块写入条带目录 `stripe-(i % Stripes)`。
- 每个块以「临时文件 + rename + fsync + 目录 fsync」原子落盘，
  并在清单中记录 `{index, stripe, length, sha256}`。
- 清单（Manifest）记录对象总大小、块数、每块元数据与整体内容摘要（所有块拼接的 SHA-256）。
- 提交通过硬链接实现「不存在才创建」的原子语义：**先到先得**。
  读取方只会看到完整清单，绝不会观察到半提交状态。
- 并发提交同一对象由对象级互斥锁序列化：
  - 内容一致的重复提交 → 幂等成功；
  - 内容不同的后提交者 → `ErrCommitConflict`；
  - 同一对象的最终结果确定：等于第一个成功提交者的内容。

## 可区分的失败原因

均可用 `errors.Is` 判定：

| 错误 | 含义 |
|---|---|
| `ErrObjectNotFound` | 对象未提交或清单不存在 |
| `ErrChunkMissing` | 清单引用的块文件缺失 |
| `ErrChunkLengthMismatch` | 块实际长度与清单不符（如被截断） |
| `ErrChecksumMismatch` | 块内容或整体摘要校验失败 |
| `ErrCommitConflict` | 同一对象已提交不同内容 |
| `ErrCheckpointCorrupt` | 检查点损坏 / CRC 不符，拒绝恢复 |
| `ErrCheckpointRegression` | 检查点回退（磁盘块数多于检查点记录），拒绝恢复 |

长度不匹配、校验失败或块缺失的对象**不会**被当作完整对象读出：
`Verify` 全量校验返回上述错误；`Get` 返回的流式读取器逐块校验，
在 `Read` 时暴露相同错误。

## 恢复语义

- 每写完一个块即原子推进检查点 `{object_id, next_chunk, bytes_written, crc}`。
- `Resume(id, r)` 从检查点位置继续：调用方从头提供数据流，
  组件跳过已完成的 `bytes_written` 字节，从 `next_chunk` 续写。
  恢复位置**不回退**，已完成的数据不重复写入、不丢失。
- 检查点 CRC 校验失败或字段非法 → `ErrCheckpointCorrupt`，
  **明确拒绝**而不是从中间继续。
- 磁盘上的连续块数多于检查点记录（检查点被回滚）→ `ErrCheckpointRegression`。
- 对象已提交时 `Resume` 幂等返回成功；无检查点时等价于全新 `Put`。

## 使用示例

```go
s, _ := store.Open("/var/mystore", store.Options{ChunkSize: 1 << 20, Stripes: 4})

// 流式写入（内存恒定）
f, _ := os.Open("big.bin")
err := s.Put("obj-1", f)        // 中断后改用 s.Resume("obj-1", f2)

// 校验与读取
err = s.Verify("obj-1")
rc, _ := s.Get("obj-1")
io.Copy(dst, rc)
rc.Close()
```

## 验证方法

```sh
go test ./store/ -v          # 功能测试，日志含块标识、检查点位置与判定依据
go test ./store/ -race       # 并发竞态检测
```

测试覆盖：

- `TestPutGetRoundtrip`：读写一致性与条带分布验证；
- `TestDistinguishableFailures`：块缺失 / 长度不匹配 / 校验失败三类原因可区分，
  且流式读取同样拒绝损坏数据；
- `TestConcurrentCommitSameContent` / `TestConcurrentCommitConflictingContent`：
  并发提交的幂等性与确定性；
- `TestInterruptAndResume`：写入中断后恢复，数据不丢不重；
- `TestResumeRejectsCorruptCheckpoint` / `TestResumeRejectsRegressedCheckpoint`：
  损坏与回退的检查点被明确拒绝；
- `TestDuplicateCommitIdempotent`：重复提交幂等，冲突重提被拒；
- `TestLargeObjectBoundedMemory`：64MiB 对象以 1MiB 块流式处理，内存有界。
