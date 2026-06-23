"""
Pulsar Lite 客户端

提供与 Milvus Lite 类似的无缝体验：
- 本地文件路径 → 自动启动嵌入式服务器
- 远程 URI → 直接连接远程服务器
"""

from pathlib import Path
from typing import Any, Optional

import pulsar

from .process_manager import process_manager


class PulsarClient:
    """
    Pulsar Lite 客户端（兼容 Pulsar Python SDK API）

    使用方式类似 Milvus Lite：
        # 嵌入式模式 - 使用本地文件自动启动
        client = PulsarClient("./demo.db")

        # 远程模式 - 连接到远程服务器
        client = PulsarClient("pulsar://localhost:6650")

        # 后续使用完全兼容 pulsar.Client API
        producer = client.create_producer("my-topic")
        producer.send(b"Hello")
    """

    def __init__(self, uri: str, **kwargs):
        """
        初始化客户端

        Args:
            uri: 连接地址，可以是：
                - 本地文件路径（如 "./demo.db"）→ 自动启动嵌入式服务器
                - Pulsar URI（如 "pulsar://localhost:6650"）→ 直接连接远程
            **kwargs: 传递给 pulsar.Client 的其他参数
        """
        self._original_uri = uri
        self._db_path: Optional[str] = None
        self._is_embedded = False
        self._client: Optional[pulsar.Client] = None

        # 判断是本地文件还是远程 URI
        is_remote = uri.startswith("pulsar://") or uri.startswith("pulsar+ssl://")

        if is_remote:
            # 远程模式：直接连接
            self._pulsar_url = uri
        else:
            # 嵌入式模式：启动本地服务器
            self._db_path = str(Path(uri).absolute())
            self._pulsar_url, self._port = process_manager.start_server(self._db_path)
            self._is_embedded = True

        # 创建实际的 Pulsar 客户端
        self._client = pulsar.Client(self._pulsar_url, **kwargs)

    def __getattr__(self, name: str) -> Any:
        """
        代理所有其他方法到 pulsar.Client

        这样用户可以使用所有标准的 Pulsar API，如：
        - create_producer
        - subscribe
        - get_topic_partitions
        - 等等...
        """
        if self._client is None:
            raise RuntimeError("Client has been closed")

        return getattr(self._client, name)

    def close(self):
        """关闭客户端并释放资源"""
        if self._client is not None:
            self._client.close()
            self._client = None

        # 如果是嵌入式模式，停止服务器
        if self._is_embedded and self._db_path:
            process_manager.stop_server(self._db_path)
            self._is_embedded = False

    def __enter__(self):
        """支持 with 语句"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出 with 语句时自动关闭"""
        self.close()
        return False

    def __del__(self):
        """析构时自动关闭"""
        try:
            self.close()
        except:
            pass

    @property
    def is_embedded(self) -> bool:
        """是否为嵌入式模式"""
        return self._is_embedded

    @property
    def db_path(self) -> Optional[str]:
        """数据库文件路径（仅嵌入式模式有效）"""
        return self._db_path

    @property
    def pulsar_url(self) -> str:
        """Pulsar 连接 URL"""
        return self._pulsar_url
