"""
Pulsar Lite - 嵌入式轻量级消息队列

使用方式类似 Milvus Lite：
    from pulsar_lite import PulsarClient

    # 嵌入式模式 - 使用本地文件自动启动
    client = PulsarClient("./milvus_demo.db")

    # 远程模式 - 连接到远程服务器
    client = PulsarClient("pulsar://localhost:6650")

    # 使用标准 Pulsar API
    producer = client.create_producer("my-topic")
    producer.send(b"Hello World!")
    client.close()
"""

__version__ = "0.2.0"

from .client import PulsarClient

__all__ = ["PulsarClient", "__version__"]
