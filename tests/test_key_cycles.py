import dataclasses
import struct
from pathlib import Path

import pytest

from rootfilespec.bootstrap import TObjString
from rootfilespec.bootstrap.TDirectory import TKeyList
from rootfilespec.reader import FileReader, open_path

DATA = Path(__file__).parent.parent / "reference" / "root-io-spec" / "data"

# Both files hold cycles 1, 2 and 3 of "str", listed in descending order, with
# the records at the offsets their case.toml pins (Record §4, Directory §9.14)
CYCLES = ["container/cycles.root", "written/cycles-3.root"]
RECORDS = {3: 468, 2: 375, 1: 282}

pytestmark = pytest.mark.skipif(
    not DATA.exists(), reason="root-io-spec submodule not checked out"
)


@pytest.mark.parametrize("name", CYCLES)
def test_every_cycle_is_a_key(name: str):
    """Issue #138: the key list is keyed by (name, cycle), so every cycle is
    reachable and the mapping agrees with itself"""
    with open_path(DATA / name) as reader:
        keylist = reader.keylist()
        assert len(keylist) == 3
        assert list(keylist) == [(b"str", 3), (b"str", 2), (b"str", 1)]
        assert len(dict(keylist)) == 3
        assert len(set(keylist.keys())) == 3
        for cycle, offset in RECORDS.items():
            key = keylist[b"str", cycle]
            assert key.header.fCycle == cycle
            assert key.fSeekKey == offset
            obj = reader.fetch(key)
            assert isinstance(obj, TObjString)
            assert obj.fString == f"revision {cycle}".encode()
        assert (b"str", 4) not in keylist
        with pytest.raises(KeyError):
            keylist[b"str", 4]


@pytest.mark.parametrize("name", CYCLES)
def test_get_by_name_is_highest_cycle(name: str):
    """Issue #138: a lookup by name alone gives the highest cycle, as ROOT's does"""
    with open_path(DATA / name) as reader:
        keylist = reader.keylist()
        key = keylist.get_by_name(b"str")
        assert key is keylist[b"str", 3]
        assert key.fSeekKey == RECORDS[3]
        with pytest.raises(KeyError):
            keylist.get_by_name(b"missing")
        # A name alone is not a key of the mapping
        with pytest.raises(KeyError):
            keylist[b"str"]  # type: ignore[index]


def test_negative_cycle_is_its_magnitude():
    """A negative fCycle is ROOT's "keep" flag, and the cycle is its magnitude
    (Record §3.8). No fixture has one, so cycle 3 of cycles.root is marked keep.
    The stored value is kept as it is."""
    with open_path(DATA / CYCLES[0]) as reader:
        key3, key2, key1 = reader.keylist().fKeys
    keep = dataclasses.replace(key3, header=dataclasses.replace(key3.header, fCycle=-3))
    keylist = TKeyList(fKeys=[keep, key2, key1], padding=b"")
    assert list(keylist) == [(b"str", 3), (b"str", 2), (b"str", 1)]
    assert keylist[b"str", 3] is keep
    assert keylist.get_by_name(b"str") is keep
    assert keep.header.fCycle == -3


def test_duplicate_cycle_raises():
    """Two keys of one name and cycle (Directory §9.14) can't both be in the
    mapping: reading such a key list raises instead of hiding one of them"""
    data = bytearray((DATA / CYCLES[0]).read_bytes())
    # The key list record at 995 (fSeekKeys) holds nKeys, then a copy of each
    # key, 66 bytes each; fCycle is at byte 16 of a short key. Make the third
    # key's cycle (1) a second cycle 2.
    (keylist_keylen,) = struct.unpack_from(">h", data, 995 + 14)
    cycle_offset = 995 + keylist_keylen + 4 + 2 * 66 + 16
    assert struct.unpack_from(">h", data, cycle_offset) == (1,)
    struct.pack_into(">h", data, cycle_offset, 2)

    reader = FileReader.open(lambda offset, size: bytes(data[offset : offset + size]))
    with pytest.raises(ValueError, match=r"two keys named b'str' with cycle 2"):
        reader.keylist()
