from __future__ import annotations

import os
import re
import time
import uuid
from pathlib import Path

import pytest


DEFAULT_BROKER_URL = "pulsar://localhost:6650"
DEFAULT_LOG_PATH = Path("/tmp/pulsar-lite.log")
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "rust" / "pulsar-lite.toml"


def _parse_int_setting(text: str, key: str, default: int) -> int:
    pattern = rf"^{re.escape(key)}\s*=\s*(\d+)\s*$"
    match = re.search(pattern, text, re.MULTILINE)
    if match is None:
        return default
    return int(match.group(1))


def _load_broker_timing(config_path: Path) -> dict[str, int]:
    if not config_path.exists():
        return {
            "keep_alive_interval_secs": 30,
            "connection_liveness_check_timeout_secs": 10,
            "handshake_timeout_secs": 30,
            "default_partitions": 0,
        }

    text = config_path.read_text(encoding="utf-8")
    return {
        "keep_alive_interval_secs": _parse_int_setting(text, "keep_alive_interval_secs", 30),
        "connection_liveness_check_timeout_secs": _parse_int_setting(
            text, "connection_liveness_check_timeout_secs", 10
        ),
        "handshake_timeout_secs": _parse_int_setting(text, "handshake_timeout_secs", 30),
        "default_partitions": _parse_int_setting(text, "default_partitions", 0),
    }


@pytest.fixture(scope="session")
def broker_url() -> str:
    return os.environ.get("PULSAR_LITE_BROKER_URL", DEFAULT_BROKER_URL)


@pytest.fixture(scope="session")
def broker_log_path() -> Path:
    return Path(os.environ.get("PULSAR_LITE_LOG_FILE", str(DEFAULT_LOG_PATH)))


@pytest.fixture(scope="session")
def broker_timing() -> dict[str, int]:
    config_path = Path(os.environ.get("PULSAR_LITE_CONFIG_FILE", str(DEFAULT_CONFIG_PATH)))
    return _load_broker_timing(config_path)


@pytest.fixture
def unique_name():
    def _make(prefix: str) -> str:
        return f"{prefix}-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"

    return _make
