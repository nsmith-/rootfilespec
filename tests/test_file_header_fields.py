import struct

import pytest

from rootfilespec.bootstrap import BOOTSTRAP_CONTEXT
from rootfilespec.bootstrap.TFile import (
    ROOTFile_header_v302,
    ROOTFile_header_v622_large,
    ROOTFile_header_v622_small,
    VersionInfo,
)
from rootfilespec.serializable import BufferContext, ReadBuffer


def _buffer(data: bytes) -> ReadBuffer:
    return ReadBuffer(memoryview(data), 0, BOOTSTRAP_CONTEXT, BufferContext(abspos=0))


@pytest.mark.parametrize(
    ("version", "large"),
    [(64004, False), (999_999, False), (1_000_000, True), (1_064_004, True)],
)
def test_large_file_flag(version: int, large: bool):
    """Issue #87: root-io-spec FileHeader §3, a version >= 1000000 is large"""
    info, _ = VersionInfo.read(_buffer(struct.pack(">i", version)))
    assert info.large is large


def test_version_numbers():
    info, _ = VersionInfo.read(_buffer(struct.pack(">i", 1_064_004)))
    assert (info.major, info.minor, info.cycle) == (6, 40, 4)


@pytest.mark.parametrize(
    ("header_class", "offset"),
    [
        (ROOTFile_header_v302, 24),
        (ROOTFile_header_v622_small, 24),
        (ROOTFile_header_v622_large, 32),
    ],
)
def test_units_is_unsigned(header_class, offset: int):
    """Issue #87: root-io-spec FileHeader §2.2, fUnits is a u8"""
    data = bytearray(200)
    data[offset] = 0x88
    header, _ = header_class.read(_buffer(bytes(data)))
    assert header.fUnits == 0x88
    assert header.fCompress == 0
