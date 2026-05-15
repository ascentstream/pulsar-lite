from __future__ import annotations

import importlib.util
import platform
from pathlib import Path

from setuptools import Distribution


def _load_setup_module():
    setup_path = Path(__file__).resolve().parents[1] / "python" / "setup.py"
    spec = importlib.util.spec_from_file_location("pulsar_lite_setup", setup_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_macos_arm64_wheel_tag_matches_bundled_binary_architecture(monkeypatch):
    setup_module = _load_setup_module()
    monkeypatch.setattr(setup_module.sysconfig, "get_platform", lambda: "macosx-10.9-universal2")
    monkeypatch.setattr(platform, "machine", lambda: "arm64")

    command = setup_module.bdist_platform_wheel(Distribution())
    command.ensure_finalized()
    command.plat_name = "macosx-10.9-universal2"

    assert command.get_tag() == ("py3", "none", "macosx_11_0_arm64")
