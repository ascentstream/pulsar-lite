#!/usr/bin/env python3
"""
Pulsar Lite 基本使用示例

演示如何使用 Pulsar Lite 作为嵌入式消息队列
支持两种模式：
1. 嵌入式模式 - 使用本地文件自动启动服务器（类似 Milvus Lite）
2. 远程模式 - 连接到独立的 Pulsar 服务器
"""

import tempfile
import pathlib
from pulsar_lite import PulsarClient


def basic_usage():
    """基本使用流程 - 嵌入式模式"""
    print("=== Pulsar Lite 基本使用示例（嵌入式模式）===\n")

    # 1. 创建临时数据库（实际使用时替换为你的路径）
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(pathlib.Path(tmpdir) / "my_queue.db")
        print(f"数据库路径: {db_path}\n")

        # 2. 创建客户端（自动启动服务器）
        # 使用 with 语句确保资源自动清理
        with PulsarClient(db_path) as client:
            print(f"✓ 已连接")
            print(f"  Pulsar URL: {client.pulsar_url}")
            print(f"  嵌入式模式: {client.is_embedded}\n")

            # 3. 创建生产者
            producer = client.create_producer("persistent://public/default/my-topic")
            print("✓ 已创建生产者: my-topic\n")

            # 4. 发送消息
            for i in range(5):
                message = f"Message {i}".encode('utf-8')
                msg_id = producer.send(message)
                print(f"  ✓ 发送消息 {i}: {msg_id}")

            print("\n✓ 所有消息发送成功！\n")

            # 5. 关闭生产者
            producer.close()
            print("✓ 生产者已关闭\n")

        # 6. 退出 with 块时自动关闭客户端和停止服务器
        print("✓ 客户端已关闭，服务器已停止\n")


def remote_mode():
    """远程模式示例 - 连接到独立服务器"""
    print("=== 远程模式示例 ===\n")

    # 连接到独立的 Pulsar Lite 服务器或 Apache Pulsar 集群
    # 需要先启动服务器: ./rust/target/release/pulsar-lite

    print("要使用远程模式：")
    print("  1. 启动服务器: ./rust/target/release/pulsar-lite")
    print("  2. 使用代码：\n")

    print("    with PulsarClient('pulsar://localhost:6650') as client:")
    print("        producer = client.create_producer('my-topic')")
    print("        producer.send(b'Hello from remote mode!')\n")

    print("✓ API 与嵌入式模式完全相同，只需修改 URI！\n")


def multiple_clients():
    """多客户端共享同一实例"""
    print("=== 多客户端共享同一实例 ===\n")

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(pathlib.Path(tmpdir) / "shared_queue.db")

        # 客户端 1 连接（启动服务器）
        print("客户端 1 正在连接...")
        client1 = PulsarClient(db_path)
        print(f"✓ 客户端 1 已连接: {client1.pulsar_url}\n")

        # 客户端 2 连接（复用同一服务器）
        print("客户端 2 正在连接...")
        client2 = PulsarClient(db_path)
        print(f"✓ 客户端 2 已连接: {client2.pulsar_url}")
        print(f"✓ 两个客户端共享同一服务器: {client1.pulsar_url == client2.pulsar_url}\n")

        # 使用客户端 1 发送消息
        producer1 = client1.create_producer("topic-1")
        producer1.send(b"Message from client 1")
        print("✓ 客户端 1 已发送消息\n")
        producer1.close()

        # 使用客户端 2 发送消息
        producer2 = client2.create_producer("topic-2")
        producer2.send(b"Message from client 2")
        print("✓ 客户端 2 已发送消息\n")
        producer2.close()

        # 客户端 1 关闭（服务器继续运行）
        print("客户端 1 正在关闭...")
        client1.close()
        print("✓ 客户端 1 已关闭（服务器仍在运行）\n")

        # 客户端 2 关闭（服务器停止）
        print("客户端 2 正在关闭...")
        client2.close()
        print("✓ 客户端 2 已关闭（服务器已停止）\n")


def multiple_databases():
    """多个独立数据库"""
    print("=== 多个独立数据库 ===\n")

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path1 = str(pathlib.Path(tmpdir) / "queue1.db")
        db_path2 = str(pathlib.Path(tmpdir) / "queue2.db")

        # 连接到第一个数据库（启动服务器 1）
        print("连接到数据库 1...")
        client1 = PulsarClient(db_path1)
        print(f"✓ 数据库 1: {client1.pulsar_url}\n")

        # 连接到第二个数据库（启动服务器 2）
        print("连接到数据库 2...")
        client2 = PulsarClient(db_path2)
        print(f"✓ 数据库 2: {client2.pulsar_url}\n")

        print(f"两个数据库使用不同的端口: {client1.pulsar_url != client2.pulsar_url}\n")

        # 发送消息到不同的数据库
        producer1 = client1.create_producer("topic-1")
        producer1.send(b"Message to database 1")
        producer1.close()
        print("✓ 已发送消息到数据库 1\n")

        producer2 = client2.create_producer("topic-2")
        producer2.send(b"Message to database 2")
        producer2.close()
        print("✓ 已发送消息到数据库 2\n")

        # 清理
        client1.close()
        client2.close()
        print("✓ 所有数据库已关闭\n")


def official_client_compatibility():
    """演示与官方 Pulsar 客户端的兼容性"""
    print("=== 官方客户端兼容性 ===\n")

    print("Pulsar Lite 完全兼容官方 Pulsar 客户端！\n")

    print("使用官方客户端连接到 Pulsar Lite：")
    print("  import pulsar")
    print("  client = pulsar.Client('pulsar://localhost:6650')")
    print("  producer = client.create_producer('my-topic')")
    print("  producer.send(b'Hello from official client!')\n")

    print("✓ 可以同时使用官方客户端和 Pulsar Lite SDK！\n")


if __name__ == "__main__":
    print("=" * 60)
    print("Pulsar Lite 使用示例")
    print("=" * 60)
    print()

    # 基本使用
    basic_usage()
    print("\n" + "=" * 60 + "\n")

    # 多客户端
    multiple_clients()
    print("\n" + "=" * 60 + "\n")

    # 多数据库
    multiple_databases()
    print("\n" + "=" * 60 + "\n")

    # 远程模式说明
    remote_mode()
    print("\n" + "=" * 60 + "\n")

    # 官方客户端兼容性
    official_client_compatibility()

    print("\n" + "=" * 60)
    print("✅ 所有示例运行完成！")
    print("=" * 60)
    print("\n下一步:")
    print("  1. 查看日志: tail -f /tmp/pulsar-lite.log")
    print("  2. 运行测试: python3 tests/test_binary_protocol.py")
    print("  3. 查看文档: cat README.md")
    print()
