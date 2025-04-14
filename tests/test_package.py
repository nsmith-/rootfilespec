import importlib.metadata

import rootfilespec as m


def test_version():
    assert importlib.metadata.version("rootfilespec") == m.__version__
