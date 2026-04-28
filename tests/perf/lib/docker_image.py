from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path
from typing import Any

from . import BROKER_BIN, ROOT

DOCKERFILE_BROKER = ROOT / "tests" / "perf" / "docker" / "Dockerfile.broker"
IMAGE_REPOSITORY = "pulsar-lite-perf"


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )


def _run_optional(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _git_dirty() -> bool:
    proc = _run(["git", "status", "--short"])
    return bool(proc.stdout.strip())


def _docker_image_id(image_tag: str) -> str:
    proc = _run(
        [
            "docker",
            "image",
            "inspect",
            image_tag,
            "--format",
            "{{.Id}}",
        ]
    )
    return proc.stdout.strip()


def _docker_image_exists(image_tag: str) -> bool:
    try:
        _docker_image_id(image_tag)
        return True
    except subprocess.CalledProcessError:
        return False


def _build_binary() -> None:
    _run(["cargo", "build", "--manifest-path", "rust/Cargo.toml", "--release"])


def _build_image(image_tag: str) -> None:
    _run(
        [
            "docker",
            "build",
            "-f",
            str(DOCKERFILE_BROKER.relative_to(ROOT)),
            "-t",
            image_tag,
            ".",
        ]
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_broker_image(
    *,
    skip_docker_build: bool = False,
) -> dict[str, Any]:
    git_dirty = _git_dirty()
    if git_dirty:
        _build_binary()

    binary_sha = _sha256_file(BROKER_BIN)
    image_tag = f"{IMAGE_REPOSITORY}:{binary_sha[:12]}"
    image_exists = _docker_image_exists(image_tag)

    build_performed = False
    build_reason = "image_exists"

    if not image_exists:
        if skip_docker_build:
            raise RuntimeError(f"Docker image {image_tag} does not exist")
        build_reason = "image_missing"
        _build_image(image_tag)
        build_performed = True

    return {
        "broker_binary_sha256": binary_sha,
        "docker_image_tag": image_tag,
        "docker_image_id": _docker_image_id(image_tag),
        "docker_build_performed": build_performed,
        "docker_build_reason": build_reason,
        "dockerfile": str(DOCKERFILE_BROKER.relative_to(ROOT)),
    }
