from pathlib import Path

import pytest
import xxhash  # type: ignore[import-not-found]

from rootfilespec.bootstrap import BOOTSTRAP_CONTEXT
from rootfilespec.reader import open_path
from rootfilespec.rntuple.pagelocations import RPageDescription, RPageLocator
from rootfilespec.rntuple.RNTuple import RNTuple
from rootfilespec.serializable import BufferContext, ReadBuffer

DATA = Path(__file__).parent.parent / "reference" / "root-io-spec" / "data" / "rntuple"
pytestmark = pytest.mark.skipif(
    not DATA.exists(), reason="reference/root-io-spec not checked out"
)


def _page_descriptions(path: Path) -> list[RPageDescription]:
    out: list[RPageDescription] = []
    with open_path(path) as reader:
        keylist = reader.keylist()
        for name in keylist:
            key = keylist[name]
            if key.fClassName.fString != b"ROOT::RNTuple":
                continue
            rntuple = RNTuple.from_anchor(reader.fetch(key), reader.fetch.buffer)
            for pagelist in rntuple.pagelistEnvelopes:
                for cluster in pagelist.pageLocations:
                    for column in cluster:
                        out.extend(column)
    return out


def _buffer(raw: bytes, offset: int, size: int) -> ReadBuffer:
    return ReadBuffer(
        memoryview(raw[offset : offset + size]),
        0,
        BOOTSTRAP_CONTEXT,
        BufferContext(abspos=offset),
    )


def test_page_with_checksum():
    """Issue #55: the page at 550 in rntuple/anchor.root, checked against its bytes

    Its fNElements is -3 (the sign flags a checksum), its locator says 12 bytes,
    and the XXH3-64 of those 12 bytes is stored little-endian at 562..570.
    """
    path = DATA / "anchor.root"
    raw = path.read_bytes()
    (page,) = [p for p in _page_descriptions(path) if p.offset == 550]
    assert page.fNElements == -3
    assert page.n_elements == 3
    assert page.has_checksum
    assert page.size == 12
    assert page.stored_size == 20

    loc = page.page_locator
    assert (loc.offset, loc.size, loc.has_checksum) == (550, 20, True)
    assert raw[562:570].hex() == "de3ce2c4a5a407be"
    read = loc.read_from(_buffer(raw, loc.offset, loc.size))
    assert read.page == raw[550:562]
    assert read.checksum == int.from_bytes(raw[562:570], "little")
    assert read.checksum == xxhash.xxh3_64_intdigest(raw[550:562])


def test_corrupted_page_raises():
    path = DATA / "anchor.root"
    raw = bytearray(path.read_bytes())
    (page,) = [p for p in _page_descriptions(path) if p.offset == 550]
    raw[555] ^= 0x01
    loc = page.page_locator
    with pytest.raises(ValueError, match="Page checksum mismatch at offset 550"):
        loc.read_from(_buffer(bytes(raw), loc.offset, loc.size))


def test_wrong_length_raises():
    loc = RPageLocator(offset=0, size=20, has_checksum=True)
    with pytest.raises(ValueError, match="expected 20 bytes"):
        loc.read_from(_buffer(bytes(12), 0, 12))


def test_page_without_checksum():
    raw = b"0123456789"
    page = RPageLocator(offset=0, size=10, has_checksum=False).read_from(
        _buffer(raw, 0, 10)
    )
    assert page.page == raw
    assert page.checksum is None


@pytest.mark.parametrize("name", sorted(p.name for p in DATA.glob("*.root")))
def test_every_page_verifies(name: str):
    """Every page of every root-io-spec RNTuple fixture passes its checksum

    Pages are read by their stored bytes, never decompressed, and several page
    descriptions may name the same bytes (same-page merging): each still reads.
    """
    path = DATA / name
    raw = path.read_bytes()
    pages = _page_descriptions(path)
    assert pages
    for page in pages:
        loc = page.page_locator
        assert loc.offset + loc.size <= len(raw)
        read = loc.read_from(_buffer(raw, loc.offset, loc.size))
        assert read.checksum is not None
        assert len(read.page) == page.size


def test_shared_page_ranges():
    """rntuple/map.root has page descriptions that name the same bytes"""
    ranges = [(p.offset, p.stored_size) for p in _page_descriptions(DATA / "map.root")]
    assert len(set(ranges)) < len(ranges)
