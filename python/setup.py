#!/usr/bin/env python3
"""Pulsar Lite setup customizations."""

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

        if plat_name in ("linux-x86_64", "linux_x86_64") and sys.maxsize == 2147483647:
            plat_name = "linux_i686"

        plat_name = plat_name.lower().replace("-", "_").replace(".", "_")
        return self.python_tag, "none", plat_name


if __name__ == "__main__":
    setup(cmdclass={"bdist_wheel": bdist_platform_wheel})
