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
    1. Python 包内置二进制（pip wheel 安装模式）
    2. 环境变量 PULSAR_LITE_BINARY
    3. 当前 Python 包目录的相对路径（开发模式）
    4. 系统 PATH
    5. 常见安装位置

    Returns:
        二进制文件的绝对路径

    Raises:
        FileNotFoundError: 找不到二进制文件
    """
    # 1. 检查 Python 包内置二进制
    package_binary_name = "pulsar-lite.exe" if sys.platform.startswith("win") else "pulsar-lite"
    packaged_binary = Path(__file__).parent / "bin" / package_binary_name
    if packaged_binary.is_file():
        return str(packaged_binary.absolute())

    # 2. 检查环境变量
    env_path = os.environ.get("PULSAR_LITE_BINARY")
    if env_path and os.path.isfile(env_path):
        return os.path.abspath(env_path)

    # 3. 开发模式：相对于当前包的路径
    package_dir = Path(__file__).parent.parent.parent.parent  # python/src/pulsar_lite -> 项目根目录
    dev_binary = package_dir / "rust" / "target" / "release" / "pulsar-lite"
    if dev_binary.exists():
        return str(dev_binary.absolute())

    # 4. 在 PATH 中查找
    binary_name = package_binary_name
    in_path = shutil.which(binary_name)
    if in_path:
        return in_path

    # 5. 常见安装位置
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
