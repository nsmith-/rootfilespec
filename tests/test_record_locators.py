"""The checks of the locators of records found by position

These locators replace ROOTFile.get_TFile, ROOTFile.get_StreamerInfo and
TDirectory.get_KeyList (#114), so each check those made is pinned here: the
key at the front of the record must say it is at the locator's offset, with
the locator's size where the size is known, in the locator's parent directory.
"""

import dataclasses
from pathlib import Path
from typing import Any

import pytest

from rootfilespec.reader import FileReader, open_path

ANCHOR = (
    Path(__file__).parent.parent
    / "reference"
    / "root-io-spec"
    / "data"
    / "rntuple"
    / "anchor.root"
)

pytestmark = pytest.mark.skipif(
    not ANCHOR.exists(), reason="root-io-spec not checked out"
)


def _locators(reader: FileReader) -> dict[str, Any]:
    return {
        "tfile": reader.file.tfile_locator,
        "streamerinfo": reader.file.streamerinfo_locator,
        "keylist": reader.rootdir.keylist_locator,
    }


@pytest.mark.parametrize("name", ["tfile", "streamerinfo", "keylist"])
def test_locators_read(name):
    with open_path(ANCHOR) as reader:
        loc = _locators(reader)[name]
        key = loc.read_from(reader.fetch.buffer(loc)).key
        assert key.fSeekKey == loc.offset


@pytest.mark.parametrize("name", ["tfile", "streamerinfo", "keylist"])
def test_offset_mismatch_raises(name):
    with open_path(ANCHOR) as reader:
        loc = _locators(reader)[name]
        buffer = reader.fetch.buffer(loc)
        moved = dataclasses.replace(loc, offset=loc.offset + 1)
        with pytest.raises(ValueError, match="(?i)fSeekKey|position"):
            moved.read_from(buffer)


@pytest.mark.parametrize("name", ["streamerinfo", "keylist"])
def test_size_mismatch_raises(name):
    """The key list's size check was only in TDirectory.get_KeyList; it fails
    on main"""
    with open_path(ANCHOR) as reader:
        loc = _locators(reader)[name]
        larger = dataclasses.replace(loc, size=loc.size + 1)
        buffer = reader.fetch.buffer(larger)
        with pytest.raises(ValueError, match="(?i)fNbytes|size"):
            larger.read_from(buffer)


def test_tfile_parent_must_be_zero():
    """The TFile's key has no parent directory"""
    with open_path(ANCHOR) as reader:
        loc = reader.file.tfile_locator
        buffer = reader.fetch.buffer(loc)
        key = loc.read_from(buffer).key
        # fSeekKey then fSeekPdir follow the 18-byte header, 4 or 8 bytes each
        width = 4 if key.header.is_short() else 8
        pdir = 18 + width
        data = bytearray(buffer.data)
        assert data[pdir : pdir + width] == bytes(width)
        data[pdir : pdir + width] = (1).to_bytes(width, "big")
        patched = dataclasses.replace(buffer, data=memoryview(bytes(data)))
        with pytest.raises(ValueError, match="parent directory"):
            loc.read_from(patched)


def test_keylist_parent_mismatch_raises():
    with open_path(ANCHOR) as reader:
        loc = reader.rootdir.keylist_locator
        buffer = reader.fetch.buffer(loc)
        moved = dataclasses.replace(loc, parent_offset=loc.parent_offset + 1)
        with pytest.raises(ValueError, match="Parent offset"):
            moved.read_from(buffer)
