from __future__ import annotations

import hashlib
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import BROKER_BIN, ROOT


def _git_output(args: list[str]) -> str | None:
    proc = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        return None
    return proc.stdout.strip()


def _sha256_file(path: Path) -> str | None:
    if not path.exists():
        return None

    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def collect_run_metadata(args: Any) -> dict[str, Any]:
    git_status = _git_output(["status", "--short"])
    binary_stat = BROKER_BIN.stat() if BROKER_BIN.exists() else None

    return {
        "git_branch": _git_output(["branch", "--show-current"]),
        "git_commit": _git_output(["rev-parse", "HEAD"]),
        "git_dirty": bool(git_status),
        "git_status_short": git_status.splitlines() if git_status else [],
        "broker_backend": args.broker_backend,
        "docker_cpuset": args.docker_cpuset,
        "docker_memory": args.docker_memory,
        "skip_docker_build": args.skip_docker_build,
        "broker_binary": {
            "path": str(BROKER_BIN.relative_to(ROOT)),
            "exists": BROKER_BIN.exists(),
            "sha256": _sha256_file(BROKER_BIN),
            "mtime": (
                datetime.fromtimestamp(binary_stat.st_mtime, timezone.utc).isoformat()
                if binary_stat
                else None
            ),
            "size_bytes": binary_stat.st_size if binary_stat else None,
        },
    }
