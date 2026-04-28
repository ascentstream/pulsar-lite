from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

PERF_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PERF_DIR))

from lib import run_metadata


def test_collect_run_metadata_records_git_state_and_binary_identity(
    monkeypatch,
    tmp_path: Path,
) -> None:
    broker_bin = tmp_path / "pulsar-lite"
    broker_bin.write_bytes(b"broker-binary")

    monkeypatch.setattr(run_metadata, "ROOT", tmp_path)
    monkeypatch.setattr(run_metadata, "BROKER_BIN", broker_bin)

    def fake_git_output(args: list[str]) -> str | None:
        if args == ["branch", "--show-current"]:
            return "perf/docker-broker-runner"
        if args == ["rev-parse", "HEAD"]:
            return "abc123"
        if args == ["status", "--short"]:
            return " M rust/src/broker.rs\n?? tests/perf/new.py"
        return None

    monkeypatch.setattr(run_metadata, "_git_output", fake_git_output)

    args = SimpleNamespace(
        broker_backend="docker",
        docker_cpuset="0-3",
        docker_memory="4g",
        skip_docker_build=False,
    )

    metadata = run_metadata.collect_run_metadata(args)

    assert metadata["git_branch"] == "perf/docker-broker-runner"
    assert metadata["git_commit"] == "abc123"
    assert metadata["git_dirty"] is True
    assert metadata["git_status_short"] == [
        " M rust/src/broker.rs",
        "?? tests/perf/new.py",
    ]
    assert metadata["broker_backend"] == "docker"
    assert metadata["docker_cpuset"] == "0-3"
    assert metadata["docker_memory"] == "4g"
    assert metadata["skip_docker_build"] is False
    assert metadata["broker_binary"]["path"] == "pulsar-lite"
    assert metadata["broker_binary"]["exists"] is True
    assert metadata["broker_binary"]["sha256"] is not None
    assert metadata["broker_binary"]["size_bytes"] == len(b"broker-binary")
