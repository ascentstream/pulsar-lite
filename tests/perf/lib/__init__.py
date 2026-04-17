from __future__ import annotations

from pathlib import Path
import os

ROOT = Path(__file__).resolve().parents[3]
PULSAR_ROOT = Path('/home/xtline/code/work/pulsar')
JAVA_HOME = Path('/usr/lib/jvm/java-17-openjdk-amd64')
JAVA = JAVA_HOME / 'bin' / 'java'
BROKER_BIN = ROOT / 'rust' / 'target' / 'release' / 'pulsar-lite'
BASE_CONFIG = ROOT / 'rust' / 'pulsar-lite.toml'
PULSAR_TESTCLIENT_JAR = PULSAR_ROOT / 'pulsar-testclient' / 'target' / 'pulsar-testclient.jar'
CLASSPATH_FILE = Path('/tmp/pulsar-testclient.classpath')

ENV_BASE = os.environ.copy()
ENV_BASE['JAVA_HOME'] = str(JAVA_HOME)
ENV_BASE['PATH'] = f"{JAVA_HOME / 'bin'}:{ENV_BASE.get('PATH', '')}"
