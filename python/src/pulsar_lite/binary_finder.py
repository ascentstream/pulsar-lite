"""
查找 Pulsar Lite 二进制文件
"""

import os
import sys
import shutil
from pathlib import Path
from typing import Optional


def find_pulsar_lite_binary() -> str:
    """
    查找 pulsar-lite 可执行文件

    搜索顺序：
    1. 环境变量 PULSAR_LITE_BINARY
    2. 当前 Python 包目录的相对路径（开发模式）
    3. 系统 PATH
    4. 常见安装位置

    Returns:
        二进制文件的绝对路径

    Raises:
        FileNotFoundError: 找不到二进制文件
    """
    # 1. 检查环境变量
    env_path = os.environ.get("PULSAR_LITE_BINARY")
    if env_path and os.path.isfile(env_path):
        return os.path.abspath(env_path)

    # 2. 开发模式：相对于当前包的路径
    package_dir = Path(__file__).parent.parent.parent.parent  # python/src/pulsar_lite -> 项目根目录
    dev_binary = package_dir / "rust" / "target" / "release" / "pulsar-lite"
    if dev_binary.exists():
        return str(dev_binary.absolute())

    # 3. 在 PATH 中查找
    binary_name = "pulsar-lite"
    in_path = shutil.which(binary_name)
    if in_path:
        return in_path

    # 4. 常见安装位置
    common_paths = [
        "/usr/local/bin/pulsar-lite",
        "/usr/bin/pulsar-lite",
        Path.home() / ".local" / "bin" / "pulsar-lite",
    ]

    for path in common_paths:
        path = Path(path)
        if path.exists():
            return str(path.absolute())

    raise FileNotFoundError(
        "Pulsar Lite binary not found. Please either:\n"
        "1. Set PULSAR_LITE_BINARY environment variable\n"
        "2. Build from source: cd rust && cargo build --release\n"
        "3. Install pulsar-lite to your PATH"
    )
