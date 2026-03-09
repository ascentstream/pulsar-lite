# Pulsar Lite Skills Index

本目录包含为 Pulsar Lite 项目定制的 Claude Code Skills。

## 可用 Skills

| Skill | 触发方式 | 用途 |
|-------|----------|------|
| `/add-command` | `/add-command` 或 "添加命令" | 添加新的 Pulsar 协议命令 |
| `/review-pulsar` | `/review-pulsar` 或 "对比Pulsar" | 对比审查与原生 Pulsar 的实现差异 |
| `/gen-test` | `/gen-test` 或 "生成测试" | 根据实现代码生成测试用例 |
| `/diagnose` | `/diagnose` 或 "诊断问题" | 诊断运行时问题 |
| `/impl-dispatcher` | `/impl-dispatcher` 或 "实现Dispatcher" | 实现新的订阅类型分发器 |

## 目录结构

```
.claude/
├── skills/                    # Skill 定义文件
│   ├── add-command.md        # 添加协议命令
│   ├── review-pulsar.md      # 对比审查实现
│   ├── gen-test.md           # 生成测试用例
│   ├── diagnose.md           # 诊断运行问题
│   └── impl-dispatcher.md    # 实现 Dispatcher
│
├── templates/                 # 代码模板
│   ├── command_handler.rs.tmpl    # Rust 处理器模板
│   ├── test_protocol.py.tmpl      # Python 测试模板
│   ├── dispatcher.rs.tmpl         # Dispatcher 模板
│   └── dispatcher_test.py.tmpl    # Dispatcher 测试模板
│
└── README.md                  # 本文件
```

## 使用示例

### 添加新协议命令

```
用户: /add-command Redeliver

Claude 将:
1. 解析 PulsarApi.proto 中的 CommandRedeliverUnacknowledgedMessages
2. 在 protocol/codec.rs 添加编解码
3. 创建 redeliver_handler.rs
4. 注册到命令路由
5. 生成测试文件
6. 更新文档
```

### 对比审查实现

```
用户: /review-pulsar shared-dispatcher

Claude 将:
1. 分析 Pulsar Lite 的 SharedDispatcher
2. 对比原生 Pulsar 的 PersistentDispatcherMultipleConsumers
3. 生成差异报告
4. 提供改进建议
```

### 生成测试用例

```
用户: /gen-test flow handler

Claude 将:
1. 分析 handle_flow 函数
2. 生成 Rust 单元测试
3. 生成 Python 集成测试
4. 包含边界条件和错误处理测试
```

### 诊断运行问题

```
用户: /diagnose 连接被拒绝

Claude 将:
1. 检查进程状态
2. 检查端口状态
3. 分析日志文件
4. 给出解决方案
```

### 实现新 Dispatcher

```
用户: /impl-dispatcher key_shared

Claude 将:
1. 分析现有 Dispatcher 实现
2. 创建 KeySharedDispatcher
3. 实现一致性哈希算法
4. 添加订阅类型支持
5. 生成测试
```

## 开发新的 Skill

### Skill 文件格式

```markdown
---
name: skill-name
description: 简短描述
triggers:
  - /skill-name
  - "触发关键词"
---

# Skill 标题

## 目标
描述 Skill 的目标

## 输入参数
- param1: 参数说明

## 执行步骤
### Step 1: 步骤名称
...

## 检查清单
- [ ] 检查项 1
- [ ] 检查项 2
```

### 最佳实践

1. **保持专注**: 每个 Skill 只做一件事
2. **提供模板**: 预定义代码模板减少重复工作
3. **验证输出**: 包含验证清单确保质量
4. **文档同步**: 更新相关文档

## 相关文档

- 项目概览: `docs/PROJECT_OVERVIEW.md`
- 协议细节: `docs/PULSAR_BINARY_PROTOCOL.md`
- 贡献指南: `docs/CONTRIBUTING.md`
- 主文档: `CLAUDE.md`
