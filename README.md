# striped-chunk-checkpoint-store-go

基于本地文件系统的分块条带存储与并发写入组件。用本地目录模拟块设备与写入检查点，
使并发提交、写入中断与恢复后的数据状态可预期且可验证。写入与读取的内存占用恒定
（单块缓冲区），不随对象大小增长。相同内容跨对象去重只存一份，支持对象删除与
可中断续跑的空间回收。

## 目录布局

```
<root>/
  data/stripe-<k>/<object-id>/chunk-NNNNNN   # 对象块文件（指向 blob 的硬链接），按 块序号 % 条带数 轮转分布
  data/blobs/<hh>/<sha256>                   # 内容寻址块（去重后的唯一副本），按哈希首字节分片
  objects/<object-id>.json                   # 已提交清单（不可变元数据）
  objects/<object-id>.tombstone              # 删除墓碑（区分"已删除"与"从未写入"）
  staging/<object-id>.checkpoint             # 写入中检查点
  gc/plan.json                               # 回收计划（回收日志，中断后续跑）
```

## 分块与提交规则

- 对象被切分为定长块（默认 64KiB，可通过 `Options.ChunkSize` 调整），
  第 `i` 块写入条带目录 `stripe-(i % Stripes)`。
- 每个块先按内容寻址写入 blob（`data/blobs/<hh>/<sha256>`，临时文件 + rename + fsync），
  再把对象块文件**硬链接**到该 blob；清单中记录 `{index, stripe, length, sha256}`。
- 清单（Manifest）记录对象总大小、块数、每块元数据与整体内容摘要（所有块拼接的 SHA-256）。
- 提交通过硬链接实现「不存在才创建」的原子语义：**先到先得**。
  读取方只会看到完整清单，绝不会观察到半提交状态。
- 并发提交同一对象由对象级互斥锁序列化：
  - 内容一致的重复提交 → 幂等成功；
  - 内容不同的后提交者 → `ErrCommitConflict`；
  - 同一对象的最终结果确定：等于第一个成功提交者的内容。

## 内容去重

- 块内容以 SHA-256 寻址：相同内容的块在磁盘上只有一份 blob，
  各对象的块文件是指向它的硬链接，**引用计数由文件系统（链接数）维护**，崩溃安全。
- 写入时 blob 已存在则跳过落盘，直接建链接——重复提交、多版本、跨对象
  的大段相同内容都不再重复占盘。
- 去重对读取完全透明：读取仍按清单逐块校验长度与内容，
  篡改或截断共享 blob 会被 `Verify` / `Get` 以 `ErrChecksumMismatch` /
  `ErrChunkLengthMismatch` 检出，不会读出残缺数据。
- 写入与读取内存占用仍为单块缓冲区，不随对象大小增长。

## 删除与空间回收

- `Delete(id)`：先落墓碑（含被删对象摘要与 CRC），再移除清单与对象块链接。
  - 删除已提交对象 → `nil`；重复删除 → 幂等 `nil`（结果确定）；
  - 删除从未写入的对象 → `ErrObjectNotFound`；
  - 删除后 `Get` / `Verify` 返回 `ErrObjectDeleted`，与"从未写入"明确区分，
    不会用残缺内容冒充完整对象；
  - 删除只移除本对象的链接，共享相同内容的其它对象不受影响；
  - 删除后可重新 `Put` 同一对象，提交成功即清除墓碑。
- `Collect()` 回收无引用空间，返回 `GCStats`（实际删除的目录数、blob 数、字节数）：
  - **安全**：blob 仅当硬链接数 == 1（无任何对象引用）才回收，且删除前在
    `gcMu` 写锁内重新校验——写入方"建 blob + 建链接"持读锁，与回收互斥，
    正在写入或读取所依赖的数据不会被回收；
  - **可续跑**：先制定回收计划落盘（`gc/plan.json`，含 CRC），再逐项执行；
    中断后再次调用自动接续未完成的计划，每项删除幂等，
    重复执行不误删、不重复计数（`GCStats.Resumed` 标识是否为续跑）；
  - **拒绝不可信记录**：计划 CRC 不符、版本非法、条目含非法哈希或
    路径穿越式对象 ID → `ErrGCPlanCorrupt`，明确拒绝并丢弃该计划，
    绝不照着它删数据；下次调用重新制定计划；
  - 被中断的 `Delete` 留下的残留块目录，由 `Collect` 在重新校验
    （墓碑有效、无清单、无写入检查点）后清理。

## 对既有数据的兼容

- 去重前的老对象（块为普通文件、无 blob）**无需迁移**：读取、校验、删除
  均正常；删除时其块文件随对象目录直接移除，空间立即释放。
- 老对象不参与去重（其内容不会反向生成 blob），新写入的对象自动去重。
- 清单格式未变，`Stripe` 字段仍按 `块序号 % 条带数` 记录，老清单可直接读取。

## 可区分的失败原因

均可用 `errors.Is` 判定：

| 错误 | 含义 |
|---|---|
| `ErrObjectNotFound` | 对象从未提交或清单不存在 |
| `ErrObjectDeleted` | 对象曾提交但已被删除 |
| `ErrTombstoneCorrupt` | 删除墓碑损坏 / CRC 不符，对象状态不可判定 |
| `ErrChunkMissing` | 清单引用的块文件缺失 |
| `ErrChunkLengthMismatch` | 块实际长度与清单不符（如被截断） |
| `ErrChecksumMismatch` | 块内容或整体摘要校验失败 |
| `ErrCommitConflict` | 同一对象已提交不同内容 |
| `ErrCheckpointCorrupt` | 检查点损坏 / CRC 不符，拒绝恢复 |
| `ErrCheckpointRegression` | 检查点回退（磁盘块数多于检查点记录），拒绝恢复 |
| `ErrGCPlanCorrupt` | 回收计划损坏或条目不可信，拒绝按它删除 |

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

// 流式写入（内存恒定，相同内容自动去重）
f, _ := os.Open("big.bin")
err := s.Put("obj-1", f)        // 中断后改用 s.Resume("obj-1", f2)

// 校验与读取
err = s.Verify("obj-1")
rc, _ := s.Get("obj-1")
io.Copy(dst, rc)
rc.Close()

// 删除与空间回收
err = s.Delete("obj-1")         // 幂等；再读得到 ErrObjectDeleted
stats, _ := s.Collect()         // 回收无引用 blob，可中断续跑
fmt.Printf("reclaimed %d bytes in %d blobs\n", stats.BytesReclaimed, stats.BlobsRemoved)
```

## 验证方法

```sh
go test ./store/ -v          # 功能测试，日志含块标识、检查点位置、回收量与判定依据
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
- `TestLargeObjectBoundedMemory`：64MiB 对象以 1MiB 块流式处理，内存有界；
- `TestDedupSharedAndOverlappingContent`：相同 / 部分重叠内容只存一份
  （日志给出 blob 数与链接数依据），读取结果与写入一致；
- `TestDedupTamperStillDetected`：篡改共享 blob 仍被逐块校验检出；
- `TestDeleteStates`：已删除 / 从未写入可区分，重复删除幂等，
  共享内容的其它对象不受影响；
- `TestDeleteThenReput`：删除后可重新写入，墓碑被清除；
- `TestGCReclaimsSpace`：删除 + 回收后磁盘占用确实下降（日志给出前后字节数），
  仍被引用的共享 blob 保留，重复回收计数为 0；
- `TestGCInterruptResume`：回收中途崩溃，重新触发接续计划干完，无重复计数；
- `TestGCRejectsCorruptPlan`：CRC 损坏与路径穿越式条目（含伪造 CRC）的计划
  被明确拒绝，数据不受影响；
- `TestGCConcurrentReadWrite`：回收与并发读写同时进行，常驻对象始终完整可读，
  被删对象状态明确；
- `TestLegacyObjectCompat`：去重前的老格式对象无需迁移，可读、可校验、可删除。
