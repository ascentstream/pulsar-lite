#!/usr/bin/env python3
"""
Pulsar Lite 使用示例

展示如何使用 Pulsar Lite 进行消息生产和消费
支持两种模式：
1. 嵌入式模式（Milvus Lite 风格）- 使用本地文件自动启动服务器
2. 远程模式 - 连接到独立的 Pulsar 服务器

特性：
- 分区 Topic 支持（PartitionedTopic）
- 独立 ledger_id（每个 Topic 独立分配）
- 完整订阅模式（Shared, Failover, Exclusive）
"""

import time

from pulsar_lite import PulsarClient


def example_partitioned_topic():
    """
    分区 Topic 示例 - 消息自动路由到多个分区

    特点：
    - Topic 自动创建多个分区
    - 消息通过 round-robin 路由到各分区
    - 每个分区有独立的 ledger_id
    - MessageId 包含 partition 字段
    """
    print("=== 分区 Topic 示例 ===")

    import pulsar

    # 连接到 Pulsar Lite
    client = pulsar.Client("pulsar://localhost:6650")

    # 创建分区 Topic 的生产者
    # Pulsar Lite 会自动创建 2 个分区
    producer = client.create_producer("persistent://public/default/partitioned-topic")

    # 发送消息到分区 Topic
    for i in range(10):
        msg_id = producer.send(f"Partitioned message {i}".encode())
        # MessageId 格式: (ledger_id, entry_id, partition, batch_index)
        print(f"  ✓ Sent to partition {msg_id[2]}: {msg_id}")

    producer.close()

    # 创建消费者订阅分区 Topic
    consumer = client.subscribe(
        "persistent://public/default/partitioned-topic",
        "partition-sub",
        consumer_type=pulsar.ConsumerType.Shared,
    )

    # 消费消息
    for i in range(5):
        msg = consumer.receive(timeout_millis=5000)
        print(f"  ✓ Received from partition {msg.message_id()[2]}: {msg.data().decode()}")
        consumer.acknowledge(msg)

    consumer.close()
    client.close()
    print("\n✅ 分区 Topic 测试成功！")


def example_embedded_mode():
    """
    嵌入式模式示例 - 自动启动本地服务器
    类似 Milvus Lite 的使用体验

    特点：
    - 指定本地文件路径即可自动启动服务器
    - 无需手动管理服务器进程
    - 适合开发、测试和演示
    """
    print("=== 嵌入式模式示例 ===")

    # 使用本地文件路径，自动启动嵌入式服务器
    # 类似 Milvus Lite: MilvusClient("./milvus_demo.db")
    with PulsarClient("./demo.db") as client:
        print(f"数据库路径: {client.db_path}")
        print(f"Pulsar URL: {client.pulsar_url}")
        print(f"嵌入式模式: {client.is_embedded}")

        # 创建生产者（完全兼容 Pulsar API）
        producer = client.create_producer("persistent://public/default/my-topic")

        # 发送消息
        for i in range(10):
            message = f"Message {i} from Pulsar Lite".encode("utf-8")
            msg_id = producer.send(message)
            print(f"  ✓ Sent message {i}: {msg_id}")
            time.sleep(0.1)

        print("\n✅ 所有消息发送成功！")

        # 关闭生产者
        producer.close()


def example_remote_mode():
    """
    远程模式示例 - 连接到远程 Pulsar 服务器

    特点：
    - 与生产环境的 Pulsar 集群无缝切换
    - API 与嵌入式模式完全相同
    - 只需修改 URI 即可在开发和生产环境间切换
    """
    print("=== 远程模式示例 ===")

    # 使用 Pulsar URI，连接到远程服务器
    # 可以是 Pulsar Lite 独立服务器或 Apache Pulsar 集群
    with PulsarClient("pulsar://localhost:6650") as client:
        print(f"Pulsar URL: {client.pulsar_url}")
        print(f"嵌入式模式: {client.is_embedded}")

        # 创建生产者
        producer = client.create_producer("persistent://public/default/remote-topic")

        # 发送消息
        message = b"Hello from Pulsar Lite!"
        msg_id = producer.send(message)
        print(f"  ✓ Sent message: {msg_id}")

        producer.close()
        print("\n✅ 远程模式测试成功！")


def example_context_manager():
    """
    使用 with 语句自动管理资源

    推荐方式：
    - 自动关闭客户端
    - 自动停止嵌入式服务器
    - 无需手动调用 close()
    """
    print("=== with 语句示例 ===")

    # 使用 with 语句，自动关闭客户端和停止服务器
    with PulsarClient("./auto_close.db") as client:
        producer = client.create_producer("test-topic")
        producer.send(b"Automatic cleanup with context manager")
        print("  ✓ 消息已发送，退出 with 块时将自动清理")
        producer.close()

    print("✅ 客户端已自动关闭，服务器已自动停止！")


def example_official_client():
    """
    使用官方 Pulsar 客户端连接到 Pulsar Lite

    演示 Pulsar Lite 完全兼容标准 Pulsar 协议
    """
    print("=== 使用官方客户端示例 ===")

    import pulsar

    # 直接使用官方 pulsar-client 连接到 Pulsar Lite
    client = pulsar.Client("pulsar://localhost:6650")

    # 创建生产者
    producer = client.create_producer("persistent://public/default/official-client-topic")

    # 发送消息
    msg_id = producer.send(b"Hello from official Pulsar client!")
    print(f"  ✓ Sent with official client: {msg_id}")

    producer.close()
    client.close()
    print("\n✅ 官方客户端兼容性测试成功！")


if __name__ == "__main__":
    # 运行示例
    print("=" * 60)
    print("Pulsar Lite 使用示例")
    print("=" * 60)

    try:
        # 示例 1: 嵌入式模式（推荐用于开发测试）
        example_embedded_mode()
        print("\n" + "=" * 60 + "\n")

        # 示例 2: with 语句自动管理
        example_context_manager()
        print("\n" + "=" * 60 + "\n")

        # 示例 3: 分区 Topic（需要先启动服务器）
        # 需要先启动服务器: ./rust/target/release/pulsar-lite
        # example_partitioned_topic()
        # print("\n" + "=" * 60 + "\n")

        # 示例 4: 使用官方客户端（演示协议兼容性）
        # 需要先启动服务器: ./rust/target/release/pulsar-lite
        # example_official_client()
        # print("\n" + "=" * 60 + "\n")

        # 示例 5: 远程模式（连接到独立服务器）
        # 需要先启动服务器: ./rust/target/release/pulsar-lite
        # example_remote_mode()
        # print("\n" + "=" * 60 + "\n")

        print("🎉 所有示例运行成功！")

    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback

        traceback.print_exc()
        print("\n提示：")
        print("  - 确保已构建 Rust Broker: cd rust && cargo build --release")
        print("  - 确保已安装 Python SDK: cd python && pip install -e .")
        print("  - 确保已安装 pulsar-client: pip install pulsar-client>=3.0.0")
