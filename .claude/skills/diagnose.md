---
name: diagnose
description: 诊断 Pulsar Lite 运行问题
triggers:
  - /diagnose
  - "诊断.*问题"
  - "连接.*失败"
  - "启动.*错误"
  - "broker.*问题"
---

# 诊断运行问题

## 目标
快速诊断和解决 Pulsar Lite 运行时问题。

## 输入参数
从用户输入中提取：
- `issue`: 问题描述 (可选，也可通过日志自动分析)
- `log_file`: 日志文件路径 (默认: /tmp/pulsar-lite.log)

## 诊断流程

### Phase 1: 收集信息

首先收集系统状态信息:

```bash
# 1. 进程状态
ps aux | grep pulsar-lite

# 2. 端口状态
lsof -i :6650
netstat -an | grep 6650

# 3. 日志检查
tail -100 /tmp/pulsar-lite.log

# 4. 存储检查
ls -la *.db 2>/dev/null
```

### Phase 2: 分类诊断

根据症状分类:

| 症状 | 可能原因 | 检查项 |
|------|----------|--------|
| 连接被拒绝 | Broker 未启动 | 进程检查、端口检查 |
| 连接超时 | 网络问题/负载高 | 网络延迟、CPU/内存 |
| 协议错误 | 版本不兼容 | 协议版本、命令格式 |
| 消息丢失 | Ack 问题 | 日志分析、存储检查 |
| 内存泄漏 | 资源未释放 | 内存监控、代码审查 |
| 性能下降 | 资源瓶颈 | CPU/磁盘/网络监控 |

### Phase 3: 详细检查

## 检查项目

### 1. 进程检查

```bash
# 检查进程是否存在
ps aux | grep -E "pulsar-lite|target/release" | grep -v grep

# 检查进程状态
ps -p <PID> -o pid,ppid,%cpu,%mem,vsz,rss,stat,start,time,command

# 检查进程树
pstree -p <PID>
```

诊断结果:
- 进程不存在 → Broker 未启动，需要启动
- 进程存在但僵死 → 检查日志，可能需要重启
- 进程正常 → 继续检查其他项

### 2. 端口检查

```bash
# 检查端口是否被监听
lsof -i :6650
netstat -tlnp | grep 6650
ss -tlnp | grep 6650

# 测试端口连通性
nc -zv localhost 6650
telnet localhost 6650
```

诊断结果:
- 端口未监听 → Broker 未启动或启动失败
- 端口被其他进程占用 → 杀掉占用进程或修改端口
- 端口正常 → 继续检查其他项

### 3. 日志分析

```bash
# 查看最近日志
tail -50 /tmp/pulsar-lite.log

# 搜索错误
grep -i error /tmp/pulsar-lite.log | tail -20
grep -i fatal /tmp/pulsar-lite.log | tail -10
grep -i exception /tmp/pulsar-lite.log | tail -20

# 搜索特定问题
grep "connection refused" /tmp/pulsar-lite.log
grep "timeout" /tmp/pulsar-lite.log
grep "failed" /tmp/pulsar-lite.log
```

常见日志错误:

| 错误关键词 | 可能原因 | 解决方案 |
|------------|----------|----------|
| `Address already in use` | 端口被占用 | 杀掉占用进程 |
| `Permission denied` | 权限不足 | 检查文件/端口权限 |
| `No such file` | 文件缺失 | 创建必要目录/文件 |
| `Out of memory` | 内存不足 | 增加内存或优化 |
| `Connection reset` | 连接异常断开 | 检查网络稳定性 |
| `Protocol error` | 协议不兼容 | 检查客户端版本 |

### 4. 存储检查

```bash
# 检查存储文件
ls -la *.db 2>/dev/null
ls -la data/ 2>/dev/null

# 检查文件权限
stat *.db 2>/dev/null

# 检查磁盘空间
df -h .
```

诊断结果:
- 存储文件不存在 → 首次运行，会自动创建
- 权限不足 → chmod/chown 修改权限
- 磁盘空间不足 → 清理磁盘空间

### 5. 网络检查

```bash
# 检查防火墙
sudo iptables -L -n | grep 6650
sudo ufw status | grep 6650

# 检查网络延迟
ping -c 3 localhost

# 检查连接数
ss -s
```

### 6. 资源检查

```bash
# CPU 使用
top -bn1 | head -5

# 内存使用
free -h

# 磁盘 IO
iostat -x 1 3
```

## 常见问题解决

### 问题 1: 连接被拒绝

**症状**:
```
pulsar.Client("pulsar://localhost:6650")
ConnectionError: Connection refused
```

**诊断步骤**:
1. 检查进程: `ps aux | grep pulsar-lite`
2. 检查端口: `lsof -i :6650`
3. 检查日志: `tail /tmp/pulsar-lite.log`

**解决方案**:
```bash
# 启动 Broker
RUST_LOG=info ./rust/target/release/pulsar-lite

# 或使用 Python SDK 自动启动
python -c "from pulsar_lite import PulsarClient; c = PulsarClient('./test.db')"
```

### 问题 2: 端口被占用

**症状**:
```
Error: Address already in use (os error 48)
```

**诊断步骤**:
1. 查找占用进程: `lsof -i :6650`
2. 确认进程信息: `ps -p <PID>`

**解决方案**:
```bash
# 杀掉占用进程
kill -9 <PID>

# 或使用其他端口
./rust/target/release/pulsar-lite --port 6651
```

### 问题 3: 协议错误

**症状**:
```
ProtocolError: Invalid command
```

**诊断步骤**:
1. 检查客户端版本
2. 检查日志中的协议错误
3. 确认命令是否已实现

**解决方案**:
- 检查 `docs/PULSAR_BINARY_PROTOCOL.md` 确认命令支持状态
- 更新客户端版本
- 检查命令格式是否正确

### 问题 4: 消息未消费

**症状**:
- 消息发送成功但消费者未收到

**诊断步骤**:
1. 检查订阅名称是否正确
2. 检查消费者是否调用了 `receive()`
3. 检查 Flow permits
4. 检查日志中的分发记录

**解决方案**:
```python
# 确保调用 flow
consumer = client.subscribe("topic", "sub")
consumer.flow(100)  # 添加 permits

# 确保循环接收
while True:
    msg = consumer.receive()
    print(msg.data())
    consumer.acknowledge(msg)
```

### 问题 5: Broker 崩溃

**症状**:
- Broker 进程意外退出

**诊断步骤**:
1. 检查日志最后内容
2. 检查是否有 panic 信息
3. 检查系统日志: `dmesg | grep pulsar`

**解决方案**:
- 提交 issue 并附带日志
- 尝试复现问题
- 检查是否是已知 bug

## 诊断报告模板

```markdown
# Pulsar Lite 诊断报告

## 环境信息
- OS: $(uname -a)
- Pulsar Lite 版本: ...
- Rust 版本: $(rustc --version)

## 问题症状
[用户描述的问题]

## 诊断结果

### 进程状态
[检查结果]

### 端口状态
[检查结果]

### 日志分析
[关键日志]

### 存储状态
[检查结果]

## 问题原因
[分析结论]

## 解决方案
1. [步骤 1]
2. [步骤 2]
3. [步骤 3]

## 预防措施
[建议]
```

## 快速诊断脚本

```bash
#!/bin/bash
# 保存为 diagnose.sh

echo "=== Pulsar Lite 诊断报告 ==="
echo "时间: $(date)"
echo ""

echo "--- 进程检查 ---"
if ps aux | grep -v grep | grep pulsar-lite > /dev/null; then
    echo "✓ Broker 进程运行中"
    ps aux | grep -v grep | grep pulsar-lite
else
    echo "✗ Broker 进程未运行"
fi
echo ""

echo "--- 端口检查 ---"
if lsof -i :6650 > /dev/null 2>&1; then
    echo "✓ 端口 6650 已监听"
    lsof -i :6650
else
    echo "✗ 端口 6650 未监听"
fi
echo ""

echo "--- 日志检查 (最近错误) ---"
if [ -f /tmp/pulsar-lite.log ]; then
    ERRORS=$(grep -i error /tmp/pulsar-lite.log | tail -5)
    if [ -n "$ERRORS" ]; then
        echo "✗ 发现错误:"
        echo "$ERRORS"
    else
        echo "✓ 无明显错误"
    fi
else
    echo "✗ 日志文件不存在"
fi
echo ""

echo "--- 存储检查 ---"
if ls *.db > /dev/null 2>&1; then
    echo "✓ 存储文件存在:"
    ls -lh *.db
else
    echo "! 未找到 .db 文件"
fi
echo ""

echo "--- 连接测试 ---"
if nc -zv localhost 6650 2>&1 | grep succeeded > /dev/null; then
    echo "✓ 端口连接成功"
else
    echo "✗ 端口连接失败"
fi
```

## 示例调用

```
用户: /diagnose 连接被拒绝

执行:
1. 检查进程状态
2. 检查端口状态
3. 分析日志
4. 给出解决方案
```

```
用户: /diagnose log_file=/tmp/custom.log

执行:
1. 分析指定日志文件
2. 识别错误模式
3. 给出诊断结论
```

## 检查清单

- [ ] 进程状态已检查
- [ ] 端口状态已检查
- [ ] 日志已分析
- [ ] 存储状态已检查
- [ ] 网络连通性已测试
- [ ] 资源使用已检查
- [ ] 问题原因已确定
- [ ] 解决方案已提供
