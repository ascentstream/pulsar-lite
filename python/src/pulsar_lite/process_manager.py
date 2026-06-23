"""
Pulsar Lite 进程管理器

参考 Milvus Lite 的实现，管理嵌入式服务器进程
支持多个客户端共享同一服务器实例
"""

import os
import socket
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple

from .binary_finder import find_pulsar_lite_binary


@dataclass
class BrokerHandle:
    """Handle for an embedded Pulsar Lite broker process."""

    db_path: str
    url: str
    port: int

    def stop(self):
        """Release this handle and stop the broker when no clients remain."""
        process_manager.stop_server(self.db_path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False


class ProcessManager:
    """
    单例进程管理器

    职责：
    1. 启动/停止 Pulsar Lite 服务器进程
    2. 引用计数：支持多个客户端共享同一实例
    3. 自动端口分配
    4. 线程安全
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._initialized = True
        self._processes: Dict[
            str, Tuple[subprocess.Popen, int, int]
        ] = {}  # db_path -> (process, ref_count, port)
        self._process_lock = threading.Lock()
        self._binary_path = None

    def _find_free_port(self, start_port: int = 6650) -> int:
        """查找可用端口"""
        port = start_port
        while port < 6700:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind(("127.0.0.1", port))
                    return port
            except OSError:
                port += 1
        raise RuntimeError("No available port found")

    def _wait_until_ready(self, process: subprocess.Popen, port: int, timeout: float = 10.0):
        """Wait until the broker accepts TCP connections on the selected port."""
        deadline = time.time() + timeout
        last_error = None

        while time.time() < deadline:
            if process.poll() is not None:
                raise RuntimeError("Pulsar Lite server exited before accepting connections")

            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                    return
            except OSError as error:
                last_error = error
                time.sleep(0.1)

        try:
            process.terminate()
            process.wait(timeout=5)
        except Exception:
            process.kill()
            process.wait()
        raise RuntimeError(f"Pulsar Lite server did not become ready on port {port}: {last_error}")

    def _is_remote_uri(self, uri: str) -> bool:
        """判断是否为远程 URI"""
        return uri.startswith("pulsar://") or uri.startswith("pulsar+ssl://")

    def start_server(self, db_path: str) -> Tuple[str, int]:
        """
        启动嵌入式服务器

        Args:
            db_path: 数据库文件路径

        Returns:
            (pulsar_url, port) - Pulsar 连接 URL 和端口号

        Raises:
            RuntimeError: 启动失败
        """
        # 规范化路径
        db_path = str(Path(db_path).absolute())

        with self._process_lock:
            # 如果已经运行，增加引用计数
            if db_path in self._processes:
                process, ref_count, port = self._processes[db_path]
                self._processes[db_path] = (process, ref_count + 1, port)
                print(
                    f"Reusing Pulsar Lite server for {db_path}, ref_count={ref_count + 1}, port={port}"
                )
                return f"pulsar://localhost:{port}", port

            # 查找二进制文件
            if self._binary_path is None:
                self._binary_path = find_pulsar_lite_binary()

            # 查找可用端口
            port = self._find_free_port()
            addr = f"127.0.0.1:{port}"

            # 确保数据库目录存在
            db_dir = Path(db_path).parent
            db_dir.mkdir(parents=True, exist_ok=True)

            # 启动进程，日志输出到 /tmp/pulsar-lite.log
            env = {**os.environ, "RUST_LOG": "info"}
            log_file = open("/tmp/pulsar-lite.log", "a")
            process = subprocess.Popen(
                [
                    self._binary_path,
                    "--addr",
                    addr,
                    "--db-path",
                    db_path,
                ],
                env=env,
                stdout=log_file,
                stderr=log_file,
                cwd=str(db_dir),
            )

            try:
                self._wait_until_ready(process, port)
            except RuntimeError as error:
                raise RuntimeError(
                    f"Pulsar Lite server failed to start for {db_path}: {error}"
                ) from error

            # 保存进程信息
            self._processes[db_path] = (process, 1, port)
            print(
                f"Started Pulsar Lite server for {db_path}, ref_count=1, port={port}, pid={process.pid}"
            )

            return f"pulsar://localhost:{port}", port

    def start_broker(self, db_path: str) -> BrokerHandle:
        """Start or reuse an embedded broker and return an explicit lifecycle handle."""
        url, port = self.start_server(db_path)
        return BrokerHandle(db_path=str(Path(db_path).absolute()), url=url, port=port)

    def stop_server(self, db_path: str):
        """
        停止嵌入式服务器（减少引用计数，为0时真正停止）

        Args:
            db_path: 数据库文件路径
        """
        db_path = str(Path(db_path).absolute())

        with self._process_lock:
            if db_path not in self._processes:
                return

            process, ref_count, port = self._processes[db_path]
            ref_count -= 1

            if ref_count == 0:
                # 引用计数为0，停止服务器
                print(f"Stopping Pulsar Lite server for {db_path}, pid={process.pid}")
                try:
                    process.terminate()
                    process.wait(timeout=5)
                except:
                    process.kill()
                    process.wait()

                del self._processes[db_path]
                print(f"Stopped Pulsar Lite server for {db_path}")
            else:
                # 更新引用计数
                self._processes[db_path] = (process, ref_count, port)
                print(f"Decreased ref_count for {db_path}, ref_count={ref_count}")

    def stop_all(self):
        """停止所有服务器"""
        with self._process_lock:
            for db_path, (process, _, _) in list(self._processes.items()):
                print(f"Stopping Pulsar Lite server for {db_path}, pid={process.pid}")
                try:
                    process.terminate()
                    process.wait(timeout=5)
                except:
                    process.kill()
                    process.wait()

            self._processes.clear()

    def __del__(self):
        self.stop_all()


# 全局单例
process_manager = ProcessManager()


def start_broker(db_path: str) -> BrokerHandle:
    """Start or reuse an embedded Pulsar Lite broker."""
    return process_manager.start_broker(db_path)
