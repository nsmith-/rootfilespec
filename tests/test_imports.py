import pkgutil
import subprocess
import sys

import pytest

import rootfilespec

MODULES = sorted(
    info.name
    for info in pkgutil.walk_packages(rootfilespec.__path__, "rootfilespec.")
    if ".generated" not in info.name
)


@pytest.mark.parametrize("module", MODULES)
def test_import_alone(module: str):
    """Issue #119: every module imports in a fresh interpreter, on its own

    Importing rootfilespec.rntuple.* (or rootfilespec.container) before
    rootfilespec.bootstrap used to fail with a circular import.
    """
    result = subprocess.run(
        [sys.executable, "-c", f"import {module}"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
