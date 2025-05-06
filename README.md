# rootfilespec

[![Actions Status][actions-badge]][actions-link]
[![Documentation Status][rtd-badge]][rtd-link]

[![PyPI version][pypi-version]][pypi-link]
[![Conda-Forge][conda-badge]][conda-link]
[![PyPI platforms][pypi-platforms]][pypi-link]

[![GitHub Discussion][github-discussions-badge]][github-discussions-link]

<!-- SPHINX-START -->

<!-- prettier-ignore-start -->
[actions-badge]:            https://github.com/nsmith-/rootfilespec/workflows/CI/badge.svg
[actions-link]:             https://github.com/nsmith-/rootfilespec/actions
[conda-badge]:              https://img.shields.io/conda/vn/conda-forge/rootfilespec
[conda-link]:               https://github.com/conda-forge/rootfilespec-feedstock
[github-discussions-badge]: https://img.shields.io/static/v1?label=Discussions&message=Ask&color=blue&logo=github
[github-discussions-link]:  https://github.com/nsmith-/rootfilespec/discussions
[pypi-link]:                https://pypi.org/project/rootfilespec/
[pypi-platforms]:           https://img.shields.io/pypi/pyversions/rootfilespec
[pypi-version]:             https://img.shields.io/pypi/v/rootfilespec
[rtd-badge]:                https://readthedocs.org/projects/rootfilespec/badge/?version=latest
[rtd-link]:                 https://rootfilespec.readthedocs.io/en/latest/?badge=latest

<!-- prettier-ignore-end -->

The rootfilespec package is designed to efficiently parse ROOT file binary data
into python datastructures. It does not drive I/O and expects materialized bytes
buffers as input. It also does not return any types beyond python dataclasses of
primitive types (and numpy arrays thereof). The goal of the project is to
provide a stable and feature-complete read/write backend for packages such as
uproot.

Further details on the design decisions can be found in
[docs/design.md](docs/design.md).
