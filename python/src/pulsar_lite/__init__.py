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

    # 显式启动 broker，然后使用官方 pulsar-client
    from pulsar_lite import start_broker
    import pulsar

    broker = start_broker("./milvus_demo.db")
    client = pulsar.Client(broker.url)
"""

__version__ = "0.1.0"

from .client import PulsarClient
from .process_manager import BrokerHandle, start_broker

__all__ = ["BrokerHandle", "PulsarClient", "start_broker", "__version__"]
