---
name: review-pulsar
description: 对比审查 Pulsar Lite 与原生 Pulsar 实现
triggers:
  - /review-pulsar
  - "对比.*Pulsar"
  - "审查.*实现"
  - "检查.*一致性"
  - "分析.*差异"
---

# 对比审查原生 Pulsar 实现

## 目标
分析 Pulsar Lite 实现与原生 Apache Pulsar 的差异，提供改进建议。

## 输入参数
从用户输入中提取：
- `module`: 要审查的模块名称
- `pulsar_path`: 原生 Pulsar 代码路径 (默认: /Users/liudezhi/git/as/pulsar)

支持的模块:
- `shared-dispatcher` / `shared` - Shared 订阅分发器
- `failover-dispatcher` / `failover` - Failover 订阅分发器
- `ack` / `acknowledgment` - 消息确认机制
- `cursor` - 游标管理
- `producer` - 生产者实现
- `consumer` - 消费者实现
- `flow-control` / `flow` - 流控机制
- `redelivery` - 重投递机制
- `storage` - 存储层

## 执行步骤

### Step 1: 识别 Pulsar Lite 实现
1. 搜索相关代码文件
2. 分析核心逻辑和设计
3. 提取关键实现点

搜索路径: `/Users/liudezhi/git/as/pulsar-lite`

### Step 2: 定位原生 Pulsar 对应实现
1. 在原生 Pulsar 代码库搜索对应类/模块
2. 分析官方实现逻辑
3. 提取关键设计点

搜索路径: `/Users/liudezhi/git/as/pulsar`

### Step 3: 对比分析

对比维度:
1. **核心算法**
   - 算法正确性
   - 时间/空间复杂度
   - 边界条件处理

2. **并发模型**
   - 线程安全机制
   - 锁粒度
   - 死锁风险

3. **错误处理**
   - 异常类型覆盖
   - 错误恢复
   - 日志完整性

4. **性能优化**
   - 批量处理
   - 内存管理
   - 异步优化

5. **可靠性机制**
   - 数据持久化
   - 故障恢复
   - 消息不丢失保证

### Step 4: 生成报告

输出格式:

```markdown
# ${module} 实现对比分析

## 1. 核心逻辑对比

### Pulsar Lite (文件路径)
\`\`\`rust
// 关键代码片段
\`\`\`

**分析**: ...

### 原生 Pulsar (文件路径)
\`\`\`java
// 对应代码片段
\`\`\`

**分析**: ...

## 2. 差异矩阵

| 特性 | Pulsar Lite | 原生 Pulsar | 影响等级 |
|------|-------------|-------------|----------|
| ... | ... | ... | 高/中/低 |

## 3. 缺失功能
- [ ] 功能1 (优先级: 高, 影响: ...)
- [ ] 功能2 (优先级: 中, 影响: ...)
- [ ] 功能3 (优先级: 低, 影响: ...)

## 4. 改进建议

### 4.1 必须实现 (高优先级)
1. **建议标题**
   - 原因: ...
   - 实现思路: ...
   - 参考代码: ...

### 4.2 建议实现 (中优先级)
...

### 4.3 可选优化 (低优先级)
...

## 5. 参考文件

### Pulsar Lite
- path/to/file1.rs
- path/to/file2.rs

### 原生 Pulsar
- path/to/File1.java
- path/to/File2.java
```

## 模块映射表

| Pulsar Lite | 原生 Pulsar | 说明 |
|-------------|-------------|------|
| `rust/src/broker/dispatcher/shared.rs` | `PersistentDispatcherMultipleConsumers.java` | Shared 分发 |
| `rust/src/broker/dispatcher/failover.rs` | `PersistentDispatcherSingleActiveConsumer.java` | Failover 分发 |
| `rust/src/storage/mod.rs` | `ManagedCursor` + `BookKeeper` | 存储/游标 |
| `rust/src/broker/service/consumer.rs` | `Consumer.java` | 消费者 |
| `rust/src/broker/service/producer.rs` | `Producer.java` | 生产者 |
| `rust/src/broker/service/topic/subscription.rs` | `PersistentSubscription.java` | 订阅管理 |
| `rust/src/protocol/codec.rs` | `Commands.java` | 协议编解码 |
| N/A | `MessageRedeliveryController.java` | 重投递控制 |
| N/A | `SharedConsumerAssignor.java` | 消息分配器 |

## 对比检查清单

### Shared Dispatcher 检查项
- [ ] Round-Robin 算法一致性
- [ ] 消费者优先级支持
- [ ] Permits 流控机制
- [ ] Pending acks 跟踪
- [ ] 消费者断开处理
- [ ] 消息重投递
- [ ] 批量大小计算

### Ack 机制检查项
- [ ] 单条确认
- [ ] 累积确认
- [ ] 游标更新
- [ ] 未确认消息计数
- [ ] 确认超时处理

### Flow Control 检查项
- [ ] Permits 管理
- [ ] Backpressure 机制
- [ ] Unacked 消息限制
- [ ] Dispatcher 阻塞/恢复

## 示例调用

```
用户: /review-pulsar shared-dispatcher

输出:
1. 分析 SharedDispatcher 实现
2. 对比 PersistentDispatcherMultipleConsumers
3. 生成差异矩阵
4. 列出缺失功能
5. 提供改进建议
```

```
用户: /review-pulsar ack

输出:
1. 分析 ack_message 实现
2. 对比原生确认机制
3. 识别缺失的累积确认
4. 建议添加 pending acks 跟踪
```

## 注意事项

1. **客观对比**: 基于代码事实，不主观臆断
2. **上下文考虑**: Pulsar Lite 是轻量级实现，不是完整复制
3. **优先级合理**: 根据实际使用场景排定优先级
4. **可操作建议**: 每个建议都应有实现思路
