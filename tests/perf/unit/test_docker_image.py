from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

PERF_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PERF_DIR))

from lib import docker_image


def _completed(args: list[str], stdout: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=args, returncode=0, stdout=stdout, stderr="")


def test_build_broker_image_reuses_existing_clean_commit_image(
    monkeypatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(args: list[str]) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        if args == ["git", "status", "--short"]:
            return _completed(args, "")
        if args[:4] == ["docker", "image", "inspect", "pulsar-lite-perf:349664eafa40"]:
            return _completed(args, "sha256:image-id\n")
        return _completed(args)

    monkeypatch.setattr(docker_image, "_run", fake_run)
    monkeypatch.setattr(docker_image, "_sha256_file", lambda path: "349664eafa409a12646bb44fba03bbbf3558fb22ae3c10ee831d4308043ce54a")

    result = docker_image.build_broker_image()

    assert calls == [
        ["git", "status", "--short"],
        [
            "docker",
            "image",
            "inspect",
            "pulsar-lite-perf:349664eafa40",
            "--format",
            "{{.Id}}",
        ],
        [
            "docker",
            "image",
            "inspect",
            "pulsar-lite-perf:349664eafa40",
            "--format",
            "{{.Id}}",
        ],
    ]
    assert result == {
        "broker_binary_sha256": "349664eafa409a12646bb44fba03bbbf3558fb22ae3c10ee831d4308043ce54a",
        "docker_image_tag": "pulsar-lite-perf:349664eafa40",
        "docker_image_id": "sha256:image-id",
        "docker_build_performed": False,
        "docker_build_reason": "image_exists",
        "dockerfile": "tests/perf/docker/Dockerfile.broker",
    }


def test_build_broker_image_builds_missing_clean_commit_image(
    monkeypatch,
) -> None:
    calls: list[list[str]] = []
    inspect_count = 0

    def fake_run(args: list[str]) -> subprocess.CompletedProcess[str]:
        nonlocal inspect_count
        calls.append(args)
        if args == ["git", "status", "--short"]:
            return _completed(args, "")
        if args[:4] == ["docker", "image", "inspect", "pulsar-lite-perf:349664eafa40"]:
            inspect_count += 1
            if inspect_count == 1:
                raise subprocess.CalledProcessError(1, args, stderr="No such image")
            return _completed(args, "sha256:image-id\n")
        return _completed(args)

    monkeypatch.setattr(docker_image, "_run", fake_run)
    monkeypatch.setattr(docker_image, "_sha256_file", lambda path: "349664eafa409a12646bb44fba03bbbf3558fb22ae3c10ee831d4308043ce54a")

    result = docker_image.build_broker_image()

    assert calls == [
        ["git", "status", "--short"],
        [
            "docker",
            "image",
            "inspect",
            "pulsar-lite-perf:349664eafa40",
            "--format",
            "{{.Id}}",
        ],
        [
            "docker",
            "build",
            "-f",
            "tests/perf/docker/Dockerfile.broker",
            "-t",
            "pulsar-lite-perf:349664eafa40",
            ".",
        ],
        [
            "docker",
            "image",
            "inspect",
            "pulsar-lite-perf:349664eafa40",
            "--format",
            "{{.Id}}",
        ],
    ]
    assert result["docker_build_performed"] is True
    assert result["docker_build_reason"] == "image_missing"


def test_build_broker_image_refreshes_dirty_binary_and_reuses_existing_image(
    monkeypatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(args: list[str]) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        if args == ["git", "status", "--short"]:
            return _completed(args, " M rust/src/broker.rs\n")
        if args[:4] == ["docker", "image", "inspect", "pulsar-lite-perf:349664eafa40"]:
            return _completed(args, "sha256:image-id\n")
        return _completed(args)

    monkeypatch.setattr(docker_image, "_run", fake_run)
    monkeypatch.setattr(docker_image, "_sha256_file", lambda path: "349664eafa409a12646bb44fba03bbbf3558fb22ae3c10ee831d4308043ce54a")

    result = docker_image.build_broker_image(skip_docker_build=True)

    assert calls == [
        ["git", "status", "--short"],
        ["cargo", "build", "--manifest-path", "rust/Cargo.toml", "--release"],
        [
            "docker",
            "image",
            "inspect",
            "pulsar-lite-perf:349664eafa40",
            "--format",
            "{{.Id}}",
        ],
        [
            "docker",
            "image",
            "inspect",
            "pulsar-lite-perf:349664eafa40",
            "--format",
            "{{.Id}}",
        ],
    ]
    assert result["docker_build_performed"] is False
    assert result["docker_build_reason"] == "image_exists"


def test_build_broker_image_errors_when_skip_requested_but_clean_image_missing(
    monkeypatch,
) -> None:
    def fake_run(args: list[str]) -> subprocess.CompletedProcess[str]:
        if args == ["git", "status", "--short"]:
            return _completed(args, "")
        if args[:4] == ["docker", "image", "inspect", "pulsar-lite-perf:349664eafa40"]:
            raise subprocess.CalledProcessError(1, args, stderr="No such image")
        return _completed(args)

    monkeypatch.setattr(docker_image, "_run", fake_run)
    monkeypatch.setattr(docker_image, "_sha256_file", lambda path: "349664eafa409a12646bb44fba03bbbf3558fb22ae3c10ee831d4308043ce54a")

    with pytest.raises(RuntimeError, match="does not exist"):
        docker_image.build_broker_image(skip_docker_build=True)
