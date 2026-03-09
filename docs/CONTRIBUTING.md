# 贡献指南

感谢你对 Pulsar Lite 的兴趣！本文档将帮助你参与项目开发。

## 快速开始

### 1. 环境要求

- **Rust**: 1.70+ (使用 `rustc --version` 检查)
- **Python**: 3.8+ (使用 `python3 --version` 检查)
- **pip**: 最新版本

### 2. Fork 并克隆仓库

```bash
# 1. 在 GitHub 上 Fork 项目
# 2. 克隆你的 Fork
git clone https://github.com/YOUR_USERNAME/pulsar-lite.git
cd pulsar-lite

# 3. 添加上游仓库
git remote add upstream https://github.com/your-org/pulsar-lite.git
```

### 3. 安装开发依赖

```bash
# 安装 Rust (如果还没有)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# 构建 Rust broker
cd rust && cargo build --release

# 安装 Python SDK (开发模式)
cd ../python
pip install -e ".[dev]"
```

### 4. 验证安装

```bash
# 运行测试
make test

# 或分别测试
make test-rust    # Rust 单元测试
make test-python  # Python 集成测试
```

## 开发工作流

### 创建功能分支

```bash
# 同步最新代码
git fetch upstream
git checkout main
git merge upstream/main

# 创建新分支
git checkout -b feature/your-feature-name
# 或
git checkout -b fix/your-bug-fix
```

### 进行开发

1. **编写代码**
   - Rust 代码: `rust/src/`
   - Python 代码: `python/src/pulsar_lite/`
   - 测试代码: `tests/`

2. **遵循代码规范**
   ```bash
   # 格式化代码
   make fmt

   # 代码检查
   make lint
   ```

3. **运行测试**
   ```bash
   # 运行所有测试
   make test

   # 运行特定测试
   cd rust && cargo test test_name
   pytest tests/test_binary_protocol.py -v
   ```

4. **更新文档** (如需要)
   - API 变更: 更新 `../README.md`
   - 架构变更: 更新 `PROJECT_OVERVIEW.md`
   - 开发指南: 更新 `../CLAUDE.md`

### 提交更改

```bash
# 查看更改
git status
git diff

# 添加文件
git add <files>

# 提交 (遵循约定式提交)
git commit -m "feat: 添加消费者订阅功能"
# 或
git commit -m "fix: 修复 macOS 上的文件锁问题"
```

#### 提交信息规范

使用约定式提交格式：

- `feat:` - 新功能
- `fix:` - Bug 修复
- `docs:` - 文档更新
- `test:` - 测试相关
- `refactor:` - 代码重构
- `perf:` - 性能优化
- `chore:` - 构建/工具链更新
- `style:` - 代码格式调整（不影响功能）

示例：
```
feat: 实现 Subscribe 和 Flow 命令
fix: 修复生产者关闭时的连接泄漏
docs: 更新 README 中的安装说明
test: 添加消息顺序测试用例
```

### 推送并创建 PR

```bash
# 推送到你的 Fork
git push origin feature/your-feature-name

# 在 GitHub 上创建 Pull Request
# 填写 PR 模板，描述更改内容和测试情况
```

## 代码规范

### Rust 代码

- **格式化**: 使用 `cargo fmt`
- **检查**: 使用 `cargo clippy`，确保无警告
- **文档**: 公共 API 必须添加文档注释 `///`
- **错误处理**: 使用 `anyhow::Result` 和 `thiserror`
- **异步**: 使用 Tokio 运行时

示例：
```rust
/// 处理 Pulsar 协议命令
///
/// # Arguments
/// * `cmd` - 协议命令
///
/// # Returns
/// 处理结果
pub async fn handle_command(cmd: BaseCommand) -> Result<ServerCommand> {
    // 实现...
}
```

### Python 代码

- **格式化**: 使用 Black (100 字符行宽)
- **检查**: 使用 Ruff (规则: E, F, I, N, W)
- **类型注解**: 所有公共函数必须添加类型注解
- **文档字符串**: 使用 Google 风格

示例：
```python
def create_producer(self, topic: str, name: Optional[str] = None) -> Producer:
    """创建生产者实例.

    Args:
        topic: Topic 名称
        name: 生产者名称（可选）

    Returns:
        Producer 实例

    Raises:
        RuntimeError: 如果连接失败
    """
    # 实现...
```

## 测试指南

### 运行测试

```bash
# 所有测试
make test

# Rust 测试
cd rust && cargo test

# Python 测试
cd python && pytest ../tests/ -v

# 特定测试
pytest tests/test_binary_protocol.py::test_produce_messages -v
```

### 测试规范

1. **单元测试**: 测试单个函数或模块
2. **集成测试**: 测试完整的用户场景
3. **协议测试**: 使用官方 Pulsar 客户端验证协议兼容性

### 测试覆盖率

我们欢迎增加测试覆盖率的 PR，特别是：
- 边界条件测试
- 错误处理测试
- 并发场景测试

## 报告问题

发现 Bug 或有功能建议？请创建 [GitHub Issue](https://github.com/your-org/pulsar-lite/issues)。

### Issue 模板

**Bug 报告**:
1. 问题描述
2. 复现步骤
3. 期望行为
4. 实际行为
5. 环境信息（OS、Python 版本、Rust 版本）
6. 日志输出（如有）

**功能请求**:
1. 功能描述
2. 使用场景
3. 期望的 API 设计（如适用）

## 开发技巧

### 查看 broker 日志

```bash
# 实时查看日志
tail -f /tmp/pulsar-lite.log

# 启动时启用调试日志
RUST_LOG=debug rust/pulsar-lite.sh start
```

### 调试 Python SDK

```python
import logging
logging.basicConfig(level=logging.DEBUG)

from pulsar_lite import PulsarClient
client = PulsarClient("./test.db")
```

### 常见问题

**Q: 构建失败，提示 protoc 未找到**
```bash
# macOS
brew install protobuf

# Ubuntu/Debian
sudo apt-get install protobuf-compiler
```

**Q: Python 测试失败，提示连接被拒绝**
```bash
# 确保 broker 正在运行
rust/pulsar-lite.sh start
sleep 2
pytest tests/test_binary_protocol.py
```

**Q: 如何清理构建缓存**
```bash
make clean
# 或
cd rust && cargo clean
rm -rf python/build python/dist
```

## 特别欢迎的贡献

我们特别欢迎以下方面的贡献：

- 🔔 **其他订阅模式** - Exclusive、Failover、Key_Shared 订阅模式
- ⚡ **性能优化** - 消息吞吐量、延迟优化
- 🧪 **测试用例** - 更多边界条件和并发测试
- 📚 **文档改进** - API 文档、使用示例、最佳实践
- 🐛 **Bug 修复** - 查看 Issues 标签
- 💾 **持久化存储** - RocksDB 集成

## 许可证

贡献的代码将以 Apache License 2.0 许可证发布。

## 获取帮助

- **GitHub Issues**: https://github.com/your-org/pulsar-lite/issues
- **查看文档**:
  - `../README.md` - 项目介绍和快速开始
  - `PROJECT_OVERVIEW.md` - 架构设计和技术细节
  - `../CLAUDE.md` - 开发指南
  - `PULSAR_BINARY_PROTOCOL.md` - 协议实现细节

感谢你的贡献！🎉
