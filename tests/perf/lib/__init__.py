from __future__ import annotations

import os
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def _path_from_env(name: str, default: Path) -> Path:
    value = os.environ.get(name)
    return Path(value).expanduser() if value else default


def _detect_java_home() -> Path | None:
    java_bin = shutil.which('java')
    if not java_bin:
        return None
    return Path(java_bin).resolve().parents[1]


PULSAR_ROOT = _path_from_env('PULSAR_ROOT', ROOT.parent / 'pulsar')
JAVA_HOME = Path(os.environ['JAVA_HOME']).expanduser() if os.environ.get('JAVA_HOME') else _detect_java_home()
JAVA = JAVA_HOME / 'bin' / 'java' if JAVA_HOME else Path('java')
BROKER_BIN = ROOT / 'rust' / 'target' / 'release' / 'pulsar-lite'
BASE_CONFIG = ROOT / 'rust' / 'pulsar-lite.toml'
PULSAR_TESTCLIENT_JAR = _path_from_env(
    'PULSAR_TESTCLIENT_JAR',
    PULSAR_ROOT / 'pulsar-testclient' / 'target' / 'pulsar-testclient.jar',
)
CLASSPATH_FILE = _path_from_env(
    'PULSAR_TESTCLIENT_CLASSPATH_FILE',
    ROOT / '.cache' / 'pulsar-testclient.classpath',
)

ENV_BASE = os.environ.copy()
if JAVA_HOME:
    ENV_BASE['JAVA_HOME'] = str(JAVA_HOME)
    ENV_BASE['PATH'] = f"{JAVA_HOME / 'bin'}:{ENV_BASE.get('PATH', '')}"

