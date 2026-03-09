---
name: add-command
description: 添加新的 Pulsar 协议命令实现
triggers:
  - /add-command
  - "添加.*命令"
  - "实现.*协议命令"
  - "新增.*Command"
---

# 添加 Pulsar 协议命令

## 目标
自动化添加新的 Pulsar 二进制协议命令，确保：
1. 遵循现有代码结构
2. 协议编解码正确
3. 处理器逻辑完整
4. 测试覆盖

## 输入参数
从用户输入中提取：
- `command_name`: 命令名称 (如 Subscribe, Flow, Ack, Redeliver)
- `command_type`: 命令类型 (自动推断: client-to-broker / broker-to-client)

## 执行步骤

### Step 1: 解析 Proto 定义
1. 读取 `rust/proto/PulsarApi.proto`
2. 查找 `message Command${command_name}` 或相关消息定义
3. 提取字段结构和类型
4. 确认命令类型 (请求/响应/单向)

检查点：
- [ ] 找到 Proto 定义
- [ ] 确认字段列表
- [ ] 确定命令方向 (client→broker 或 broker→client)

### Step 2: 添加协议编解码
编辑 `rust/src/protocol/codec.rs`：

1. **Client-to-Broker 命令**: 在 `ClientCommand` 枚举添加变体
```rust
pub enum ClientCommand {
    // ... 现有命令
    ${command_name} {
        request_id: u64,
        // 根据 Proto 字段添加
    },
}
```

2. **Broker-to-Client 命令**: 在 `ServerCommand` 枚举添加变体
```rust
pub enum ServerCommand {
    // ... 现有命令
    ${command_name} {
        request_id: u64,
        // 根据 Proto 字段添加
    },
}
```

3. 在 `decode_command` 函数添加解码分支
4. 在 `encode_command` 函数添加编码分支

检查点：
- [ ] 枚举变体已添加
- [ ] 解码逻辑已实现
- [ ] 编码逻辑已实现
- [ ] 编译通过 `cargo check`

### Step 3: 创建处理器
1. 创建文件 `rust/src/broker/handler/${command_name_lower}_handler.rs`
2. 实现处理函数 `handle_${command_name_lower}`

处理器模板见: `.claude/templates/command_handler.rs.tmpl`

检查点：
- [ ] 处理器文件已创建
- [ ] 处理函数已实现
- [ ] 日志已添加

### Step 4: 注册处理器
1. 编辑 `rust/src/broker/handler/mod.rs`:
```rust
mod ${command_name_lower}_handler;
pub use ${command_name_lower}_handler::handle_${command_name_lower};
```

2. 编辑 `rust/src/broker/service/server_cnx.rs`:
在 `handle_command` 方法添加路由:
```rust
ClientCommand::${command_name} { .. } => {
    ${command_name_lower}_handler::handle_${command_name_lower}(self, cmd).await
}
```

检查点：
- [ ] 模块已导出
- [ ] 路由已添加

### Step 5: 生成测试
创建文件 `tests/test_${command_name_lower}.py`

测试模板见: `.claude/templates/test_protocol.py.tmpl`

检查点：
- [ ] 测试文件已创建
- [ ] 基本测试用例已添加

### Step 6: 更新文档
编辑 `docs/PULSAR_BINARY_PROTOCOL.md`:

添加命令状态:
```markdown
| ${command_name} | ${status} | ${notes} |
```

检查点：
- [ ] 协议文档已更新

### Step 7: 验证
运行测试确保一切正常：
```bash
cd rust && cargo test
cd .. && pytest tests/test_${command_name_lower}.py -v
```

检查点：
- [ ] Rust 单元测试通过
- [ ] Python 集成测试通过

## 命令类型参考

| 命令 | 类型 | Proto 消息 |
|------|------|------------|
| Connect | 双向 | CommandConnect / CommandConnected |
| Subscribe | 请求-响应 | CommandSubscribe / CommandSuccess |
| Flow | 单向 | CommandFlow |
| Send | 单向 | CommandSend |
| Ack | 单向 | CommandAck |
| CloseProducer | 请求-响应 | CommandCloseProducer |
| CloseConsumer | 请求-响应 | CommandCloseConsumer |
| Ping/Pong | 单向 | CommandPing / CommandPong |

## 常见字段

| 字段 | 类型 | 说明 |
|------|------|------|
| request_id | u64 | 请求标识，用于匹配响应 |
| producer_id | u64 | 生产者标识 |
| consumer_id | u64 | 消费者标识 |
| topic | String | Topic 名称 |
| subscription | String | 订阅名称 |
| message_id | MessageIdData | 消息 ID |

## 示例调用

```
用户: /add-command Redeliver

执行:
1. 解析 CommandRedeliverUnacknowledgedMessages
2. 在 ClientCommand 添加变体
3. 创建 redeliver_handler.rs
4. 注册到 mod.rs 和 server_cnx.rs
5. 生成 test_redeliver.py
6. 更新协议文档
7. 运行测试验证
```

## 完成清单

- [ ] Proto 定义已解析
- [ ] 编解码已实现
- [ ] Handler 已创建并注册
- [ ] 路由已添加
- [ ] 测试已生成
- [ ] 文档已更新
- [ ] `cargo test` 通过
- [ ] `pytest tests/` 通过
