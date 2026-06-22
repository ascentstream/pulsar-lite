#!/usr/bin/env python3
"""Validate Conventional Commit messages."""

from __future__ import annotations

import re
import sys
from pathlib import Path


ALLOWED_TYPES = {
    "feat",
    "fix",
    "docs",
    "test",
    "refactor",
    "perf",
    "chore",
    "style",
    "ci",
    "build",
    "revert",
}

HEADER_PATTERN = re.compile(
    r"^(?P<type>[a-z]+)(\([a-z0-9._/-]+\))?(?P<breaking>!)?: (?P<description>.+)$"
)


def is_allowed_exception(header: str) -> bool:
    return (
        header.startswith("Merge ")
        or header.startswith("Revert ")
        or header.startswith("fixup! ")
        or header.startswith("squash! ")
    )


def validate_header(header: str) -> str | None:
    if is_allowed_exception(header):
        return None

    match = HEADER_PATTERN.match(header)
    if match is None:
        return (
            "Commit message must use Conventional Commits: "
            "type(optional-scope)!: description"
        )

    commit_type = match.group("type")
    if commit_type not in ALLOWED_TYPES:
        allowed = ", ".join(sorted(ALLOWED_TYPES))
        return f"Unsupported commit type '{commit_type}'. Allowed types: {allowed}"

    description = match.group("description").strip()
    if not description:
        return "Commit description must not be empty"

    if description.endswith("."):
        return "Commit description should not end with a period"

    return None


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: check_commit_msg.py <commit-msg-file>", file=sys.stderr)
        return 2

    message_path = Path(sys.argv[1])
    if not message_path.is_file():
        print(f"commit message file not found: {message_path}", file=sys.stderr)
        return 2

    message = message_path.read_text(encoding="utf-8")
    header = next(
        (line.strip() for line in message.splitlines() if line.strip() and not line.startswith("#")),
        "",
    )

    error = validate_header(header)
    if error is None:
        return 0

    print(f"Invalid commit message: {header!r}", file=sys.stderr)
    print(error, file=sys.stderr)
    print(file=sys.stderr)
    print("Examples:", file=sys.stderr)
    print("  feat(broker): add persistent key-shared dispatch", file=sys.stderr)
    print("  fix(storage): preserve cursor state across restart", file=sys.stderr)
    print("  docs: rewrite repository README", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
