# striped-chunk-checkpoint-store-go

基于本地文件系统的分块条带存储与并发写入组件。用本地目录模拟块设备与写入检查点，
使并发提交、写入中断与恢复后的数据状态可预期且可验证。写入与读取的内存占用恒定
（单块缓冲区），不随对象大小增长。

在分块写入、并发提交、断点恢复之上，进一步提供：

- **内容去重（CAS）**：相同内容块在磁盘上只存一份，跨对象、跨版本、对象内部重复均可共享；
- **对象删除**：`Delete` 幂等删除，用墓碑把"已删除"与"从未写入"严格区分；
- **空间回收**：`Reclaim` 标记-清除无引用块，扛并发、可中断续跑、计划损坏明确拒绝。

## 目录布局

```
<root>/
  data/stripe-<k>/<object-id>/chunk-NNNNNN   # 写入暂存块（提交后即释放）
                                             # 或 v1 老对象的私有块
  cas/sha256/<前2位>/<64位哈希>              # 内容寻址仓库：去重后的唯一物理块
  objects/<object-id>.json                   # 已提交清单（不可变元数据）
  tombstones/<object-id>.json                # 已删除对象的墓碑
  staging/<object-id>.checkpoint             # 写入中检查点
  gc/active.plan.json                        # 当前回收计划（带 CRC）
  gc/<plan-id>.done/<前2位>/<哈希>           # 逐块回收完成标记（中断续跑依据）
```

## 内容去重（CAS）

- 每个块以其**内容 SHA-256** 命名存入 `cas/sha256/`，是磁盘上的唯一物理副本。
- 对象仍按条带**流式写入暂存目录**（恒定单块缓冲，内存不随对象大小增长）；
  提交时逐块用硬链接提升进 CAS：内容相同的块（无论来自同一对象还是不同对象、
  新提交还是多版本）都指向同一物理文件，不重复占盘。
- 清单 `ChunkMeta` 增加 `ref` 字段记录内容块的哈希；读取与逐块校验逻辑不变，
  因此**去重不改变读取结果**：共享块与独占块读出内容都与写入逐字节一致，
  篡改或截断仍会被长度 / 块摘要 / 整体摘要校验发现。
- 去重粒度是"块"：两个对象只要有相同的块（含大块公共前缀）就只存一份，
  差异部分各自独立。
- **块边界稳定性**：只有完整块才落盘并推进检查点；崩溃时的尾部不足块会被
  丢弃并在恢复时重写。因此中断后续写与全新写入产出逐字节相同的块哈希，
  断点恢复的对象也能正常参与去重。

## 对象删除

- `Delete(id)` 删除已提交对象，语义明确且幂等：

  | 情形 | 结果 |
  |---|---|
  | 对象存在且已提交 | 先原子写墓碑、再移除清单与私有暂存；成功 |
  | 对象此前已删除（有墓碑） | 幂等成功，可反复删除 |
  | 对象从未写入（无清单无墓碑） | `ErrObjectNotFound` |
  | 删除后再读 | `ErrObjectDeleted`（与"从未写过"严格区分） |

- 删除**绝不用残缺内容冒充完整对象**：墓碑落盘后读取一律得到 `ErrObjectDeleted`。
- 已删除的对象 ID 不能被新的 `Put`/`Resume` 覆盖（返回 `ErrObjectDeleted`），
  保证删除结论稳定、可区分。
- 删除共享对象**不影响**仍引用相同内容的其它对象：v2 对象的共享块由
  回收器按全局引用关系释放，删除本身只移除清单与该对象的暂存；
  v1 老对象的块为其独占，删除时直接释放。

## 空间回收（Reclaim）

- `Reclaim()` 采用**标记-清除（mark-sweep）**两阶段，专门释放
  "不再被任何存活对象引用"的 CAS 块：
  1. **标记**：扫描所有存活清单，汇总被引用的内容块集合，与 CAS 仓库求差集，
     结果写入带 CRC 的计划文件 `gc/active.plan.json`；
  2. **清除**：逐项删除计划块；每删一块就原子写一个 done 标记。
- **并发安全**：回收全程持有生命周期写锁，与正在进行的读取、提交、删除互斥；
  `Get` 的读取器在 `Close` 前持有读锁，因此**正在读取 / 正在提交所依赖的
  数据不会被回收**。提交窗口（块进 CAS → 清单发布）也持读锁，杜绝
  "块已就位但清单尚不可见"时被误判为垃圾。
- **中断续跑 / 幂等**：清除到一半被打断，重新调用 `Reclaim()` 会读取计划
  接着干完；已写 done 标记的块跳过，不重复删除、不重复计数。
  清除前还会**重新扫描一次存活引用**，计划生成后重新获得引用的块被跳过，
  因此重复执行不会误删仍有引用的数据。全部完成后计划与标记目录被清理，
  下一次回收重新标记。
- **防篡改（fail-closed）**：计划记录包含每个块的哈希、长度、总字节与整体
  CRC。计划损坏、字段不自洽或 CRC 不符时返回 `ErrReclaimPlanCorrupt`，
  **明确拒绝执行删除**，绝不照不可信记录删数据；任何存活清单无法解析也会
  中止回收（宁可少回收，也不误删）。
- 返回的 `ReclaimReport{DeletedBlocks, ReclaimedBytes, SkippedBlocks, Resumed}`
  可直接核对：`ReclaimedBytes` 等于 CAS 目录前后的实际字节差。

## 与既有数据的兼容

- **零迁移可读**：升级后 `Open` 会自动补齐 `cas/`、`tombstones/`、`gc/` 目录。
  旧版本写入的清单（无 `version`、块无 `ref`）按 v1 布局从对象私有条带目录
  读取，行为与升级前完全一致，逐块校验照常生效。
- 老对象**不强制迁移**，因此默认不参与去重；删除老对象时其独占私有块会被
  直接释放。新写入（含断点恢复后提交）一律使用 v2 CAS 布局。新旧格式可在
  同一存储内长期共存。
- 若希望老对象也享受去重，可用相同内容重新 `Put` 一个**新对象 ID**；
  已删除 ID 不可复用（见上）。

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
| `ErrCheckpointRegression` | 检查点回退（磁盘完整块数多于检查点记录），拒绝恢复 |
| `ErrObjectDeleted` | 对象曾经提交但已被删除（区别于从未写入） |
| `ErrReclaimPlanCorrupt` | 回收计划损坏 / CRC 不符 / 不可信，拒绝据此删除 |

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

// 删除（幂等；从未写入返回 ErrObjectNotFound，已删除读取返回 ErrObjectDeleted）
err = s.Delete("obj-1")

// 空间回收：标记-清除，可中断续跑；返回实际删除的块数与字节
rep, err := s.Reclaim()
// rep.DeletedBlocks / rep.ReclaimedBytes / rep.SkippedBlocks / rep.Resumed
```

## 验证方法

```sh
go test ./store/ -v          # 功能测试，日志含共享块数、回收字节、保留依据等
go test ./store/ -race       # 并发竞态检测（含回收 × 读写并发）
```

测试覆盖（日志会打印实际 CAS 块数、回收前后字节、共享/保留的判定依据）：

去重：

- `TestDedupIdenticalObjectsShareBlocks`：大段相同的两对象共享物理块，且读出与写入逐字节一致；
- `TestDedupSameObjectRepeatedChunks`：单对象内部重复块只存一份；
- `TestDedupStreamingBoundedMemory`：两个 64MiB 相同对象只占一份，读写内存有界；
- `TestResumeThenCommitUsesDedup`：中断恢复后提交的对象进入 CAS，与相同内容对象零增量共享。

删除：

- `TestDeleteDistinguishableFromNotFound`：删除后 `ErrObjectDeleted`，从未写入 `ErrObjectNotFound`，
  且物理上有墓碑、无清单；
- `TestDeleteIdempotentAndNotFound`：反复删除幂等；删除不存在对象报错；已删 ID 不可被覆盖；
- `TestDeleteDoesNotAffectSharingObject`：删除共享者之一，其它对象仍完整，独有块随后被回收。

空间回收：

- `TestReclaimFreesOnlyUnreferencedSpace`：只回收无引用块，回收字节 == CAS 实际下降字节，
  存活对象的每个 ref 仍在盘上（保留依据），重复回收不重复计数；
- `TestReclaimInterruptedResumes`：清除中途注入崩溃，done 标记落盘，重新 `Reclaim` 续跑
  （`Resumed=true`），只补做未完成项，最终 CAS 清空、计划清理；
- `TestReclaimRejectsCorruptPlan`：篡改计划后回收返回 `ErrReclaimPlanCorrupt`，一个字节都不删。

回收 × 并发读写 / 中断：

- `TestReclaimConcurrentWithRead`：读取窗口打开时回收被阻塞，窗口内共享块完好，窗口关闭后才回收；
- `TestReclaimConcurrentWithCommit`：12 个提交与 12 个回收并发，完成后所有新对象逐块校验可读；
- `TestDeleteConcurrentWithReaders`：活跃读取期间两个并发删除都等待，读取结束后删除幂等完成。

兼容性：

- `TestBackwardCompatReadsV1ObjectsWithoutMigration`：手工构造 v1 老存储，升级后零迁移可读、
  可校验、可重复提交、可删除，新对象走 v2，新旧共存。

原有分块 / 并发提交 / 中断恢复用例（`TestPutGetRoundtrip`、`TestInterruptAndResume`、
`TestConcurrentCommit*` 等）全部继续通过，无回归。
