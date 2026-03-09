---
name: gen-test
description: 根据实现代码生成测试用例
triggers:
  - /gen-test
  - "生成.*测试"
  - "为.*写测试"
  - "添加.*测试"
---

# 生成测试用例

## 目标
分析实现代码，自动生成高质量的测试用例，提高测试覆盖率。

## 输入参数
从用户输入中提取：
- `target`: 目标模块/文件/功能
- `test_type`: 测试类型 (可选: unit/integration/protocol)
- `language`: 测试语言 (可选，根据目标自动推断: rust/python)

## 执行步骤

### Step 1: 分析目标代码
1. 读取目标文件
2. 识别公共接口 (pub fn, pub async fn)
3. 分析函数签名和返回类型
4. 提取分支逻辑 (if/else, match)
5. 识别边界条件

分析要点:
- 函数名和参数
- 返回类型 (Result, Option, 等)
- 错误类型
- 依赖项

### Step 2: 生成测试场景

根据代码逻辑生成以下测试场景:

1. **正常流程 (Happy Path)**
   - 有效输入
   - 预期输出
   - 无错误发生

2. **边界条件**
   - 空输入 (空字符串、空列表、None)
   - 极值 (最大值、最小值、零值)
   - 特殊字符/格式

3. **错误处理**
   - 无效输入
   - 资源不存在
   - 权限不足
   - 超时

4. **并发场景** (如适用)
   - 多线程访问
   - 竞态条件
   - 死锁检测

5. **协议兼容** (针对网络/协议代码)
   - 与官方客户端交互
   - 协议版本兼容
   - 编解码正确性

### Step 3: 生成测试代码

根据语言选择对应框架:

**Rust 测试** (单元测试):
- 框架: `#[test]`, `#[tokio::test]`
- 断言: `assert!`, `assert_eq!`, `assert_ne!`
- Mock: `mockall` (如需要)
- 位置: 文件末尾 `#[cfg(test)] mod tests`

**Python 测试** (集成测试):
- 框架: `pytest`
- 客户端: `pulsar-client` (官方)
- Fixture: `@pytest.fixture`
- 位置: `tests/test_*.py`

### Step 4: 生成测试数据

构造测试所需的数据:
- Mock 对象
- 测试 Fixtures
- 测试配置
- 清理逻辑

### Step 5: 验证测试

生成后运行测试确保:
- 测试可执行
- 覆盖目标场景
- 无误报

## Rust 测试模板

### 同步函数测试
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_${function_name}_success() {
        // Arrange
        let input = /* 测试输入 */;

        // Act
        let result = ${function_name}(input);

        // Assert
        assert!(result.is_ok());
        // 或 assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn test_${function_name}_invalid_input() {
        let result = ${function_name}(/* 无效输入 */);
        assert!(result.is_err());
    }

    #[test]
    fn test_${function_name}_boundary() {
        // 边界条件测试
    }
}
```

### 异步函数测试
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_${function_name}_async_success() {
        // Arrange
        let input = /* ... */;

        // Act
        let result = ${function_name}(input).await;

        // Assert
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_${function_name}_concurrent() {
        // 并发测试
        use std::sync::Arc;
        use tokio::sync::Mutex;

        let shared = Arc::new(Mutex::new(0));
        let mut handles = vec![];

        for _ in 0..10 {
            let shared = shared.clone();
            handles.push(tokio::spawn(async move {
                // 并发操作
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }
}
```

## Python 测试模板

### 基础测试
```python
import pytest
import pulsar


@pytest.fixture(scope="module")
def client():
    """Create Pulsar client"""
    c = pulsar.Client("pulsar://localhost:6650")
    yield c
    c.close()


@pytest.fixture(scope="module")
def producer(client):
    """Create producer for testing"""
    p = client.create_producer("test-topic")
    yield p
    p.close()


class Test${FeatureName}:
    """Test suite for ${feature_name}"""

    def test_${feature}_basic(self, client):
        """Test basic ${feature} functionality"""
        # Arrange
        # Act
        # Assert
        pass

    def test_${feature}_with_parameters(self, client):
        """Test ${feature} with various parameters"""
        pass

    def test_${feature}_error_handling(self, client):
        """Test error scenarios"""
        with pytest.raises(Exception):
            # 触发错误的操作
            pass
```

### 协议测试
```python
class Test${CommandName}Protocol:
    """Protocol-level tests for ${command_name}"""

    def test_binary_protocol_compatibility(self):
        """Verify binary protocol compatibility"""
        # 使用官方客户端验证协议兼容
        client = pulsar.Client("pulsar://localhost:6650")

        # 执行协议操作
        # 验证响应

        client.close()

    def test_command_serialization(self):
        """Test command serialization format"""
        # 验证序列化正确性
        pass
```

### Dispatcher 测试
```python
class Test${DispatcherType}Dispatcher:
    """Test dispatcher behavior"""

    def test_round_robin_distribution(self):
        """Test messages are distributed in round-robin"""
        import pulsar

        client = pulsar.Client("pulsar://localhost:6650")
        producer = client.create_producer("test-rr-topic")

        # 创建多个消费者
        consumers = []
        for i in range(3):
            c = client.subscribe(
                "test-rr-topic",
                "test-sub",
                consumer_name=f"consumer-{i}"
            )
            consumers.append(c)

        # 发送多条消息
        for i in range(30):
            producer.send(f"message-{i}".encode())

        # 验证分布
        # ...

        producer.close()
        for c in consumers:
            c.close()
        client.close()
```

## 覆盖率目标

| 类型 | 目标覆盖率 |
|------|-----------|
| 语句覆盖 | > 80% |
| 分支覆盖 | > 70% |
| 关键路径 | 100% |
| 错误处理 | > 60% |

## 示例调用

```
用户: /gen-test shared dispatcher

输出:
1. 分析 SharedDispatcher 实现
2. 生成 Rust 单元测试
3. 生成 Python 集成测试
4. 包含 Round-Robin 验证
5. 包含流控测试
```

```
用户: /gen-test rust/src/broker/handler/flow_handler.rs

输出:
1. 分析 handle_flow 函数
2. 生成测试用例:
   - test_handle_flow_success
   - test_handle_flow_invalid_consumer
   - test_handle_flow_zero_permits
```

```
用户: /gen-test ack mechanism integration

输出:
1. 生成 Python 集成测试
2. 测试场景:
   - 单条确认
   - 批量确认
   - 确认后游标更新
```

## 测试命名规范

| 类型 | 命名格式 |
|------|----------|
| 正常流程 | `test_{function}_{scenario}` |
| 错误处理 | `test_{function}_error_{error_type}` |
| 边界条件 | `test_{function}_boundary_{condition}` |
| 并发 | `test_{function}_concurrent` |

## 检查清单

- [ ] 目标代码已分析
- [ ] 测试场景已识别
- [ ] 测试代码已生成
- [ ] Mock/Fixture 已创建
- [ ] 测试可执行
- [ ] 覆盖主要分支
