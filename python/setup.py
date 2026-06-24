#!/usr/bin/env python3
"""Pulsar Lite setup customizations."""

import platform
import sys
import sysconfig

from setuptools import setup

try:
    from setuptools.command.bdist_wheel import bdist_wheel
except ImportError:
    from wheel.bdist_wheel import bdist_wheel


class bdist_platform_wheel(bdist_wheel):
    """Build py3-none-platform wheels for the bundled Rust broker binary."""

    def finalize_options(self):
        super().finalize_options()
        self.root_is_pure = False

    def get_tag(self):
        if self.plat_name_supplied:
            plat_name = self.plat_name
        elif self.plat_name and not self.plat_name.startswith("macosx"):
            plat_name = self.plat_name
        else:
            plat_name = sysconfig.get_platform()
            plat_name = _native_macos_platform(plat_name)

        # Normalize to PEP 425 form (lowercase, underscores, no dots).
        plat_name = plat_name.lower().replace("-", "_").replace(".", "_")

        # PyPI rejects bare "linux_*" platform tags (PEP 513/599/600). The
        # bundled Rust broker is a static binary with no glibc/ELIB coupling,
        # so we relabel Linux wheels to manylinux_2_17 which is accepted by
        # PyPI and works on any glibc >= 2.17 (CentOS 7+) distribution.
        if plat_name == "linux_x86_64":
            plat_name = "manylinux_2_17_x86_64"
        elif plat_name == "linux_i686":
            plat_name = "manylinux_2_17_i686"
        elif plat_name == "linux_aarch64":
            plat_name = "manylinux_2_17_aarch64"

        # 32-bit x86 fallback for legacy i686 detection.
        if plat_name in ("linux-x86_64", "linux_x86_64") and sys.maxsize == 2147483647:
            plat_name = "manylinux_2_17_i686"

        return self.python_tag, "none", plat_name


def _native_macos_platform(plat_name):
    if not plat_name.startswith("macosx") or "universal2" not in plat_name:
        return plat_name

    machine = platform.machine().lower()
    if machine in ("arm64", "aarch64"):
        return "macosx-11.0-arm64"
    if machine in ("x86_64", "amd64"):
        return "macosx-10.9-x86_64"
    return plat_name


if __name__ == "__main__":
    setup(cmdclass={"bdist_wheel": bdist_platform_wheel})
