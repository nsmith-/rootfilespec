"""Non-standard locators, read as ROOT reads them (#122)

ROOT writes a non-standard locator's first word as the negation of
(type << 24 | reserved << 16 | size), where the size counts the word itself,
and reads it back by negating it before splitting it
(RNTupleSerializer::SerializeLocator and DeserializeLocator,
tree/ntuple/src/RNTupleSerialize.cxx:1068-1155 at 6.40.04). A type it doesn't
know becomes kTypeUnknown, so the page list holding it still reads; only
reading what it locates fails.
"""

from pathlib import Path

import pytest
import xxhash  # type: ignore[import-not-found]

from rootfilespec.bootstrap import BOOTSTRAP_CONTEXT, ROOT3a3aRNTuple
from rootfilespec.bootstrap.TFile import InitialReadLocator
from rootfilespec.reader import Fetcher
from rootfilespec.rntuple.envelope import REnvelopeLink
from rootfilespec.rntuple.pagelist import PageListEnvelope
from rootfilespec.rntuple.pagelocations import RPageDescription
from rootfilespec.rntuple.RLocator import (
    LargeLocator,
    RLocator,
    StandardLocator,
    UnknownLocator,
)
from rootfilespec.serializable import BufferContext, ReadBuffer

ANCHOR = (
    Path(__file__).parent.parent
    / "reference"
    / "root-io-spec"
    / "data"
    / "rntuple"
    / "anchor.root"
)


def _head(locator_type: int, payload_size: int, reserved: int = 0) -> bytes:
    """A non-standard locator's first word, as SerializeLocator writes it"""
    head = (locator_type & 0x7F) << 24 | reserved << 16 | (4 + payload_size)
    return (-head).to_bytes(4, "little", signed=True)


def _read(data: bytes) -> tuple[RLocator, ReadBuffer]:
    buffer = ReadBuffer(memoryview(data), 0, BOOTSTRAP_CONTEXT, BufferContext(abspos=0))
    return RLocator.read(buffer)


def _object64(nbytes: int, location: int) -> bytes:
    """An Object64 payload with a 32 bit size (SerializeLocatorPayloadObject64)"""
    return nbytes.to_bytes(4, "little") + location.to_bytes(8, "little")


def test_large_locator():
    """A large locator as ROOT writes it. On main its type reads as 0xfe (the
    top byte of the negated word) and it raises"""
    payload = (5 << 32).to_bytes(8, "little") + (1234).to_bytes(8, "little")
    locator, rest = _read(_head(0x01, 16) + payload + b"next")
    assert locator == LargeLocator(size=5 << 32, offset=1234)
    assert bytes(rest.data) == b"next"


def test_large_locator_of_another_size_raises():
    with pytest.raises(ValueError, match="Large locator payload is 12 bytes"):
        _read(_head(0x01, 12) + bytes(12))


@pytest.mark.parametrize(
    ("locator_type", "payload"),
    [
        # The test locator, with the Object64 payload ROOT's tests give it
        (0x7E, _object64(0, 0)),
        # DAOS, with a 32 and a 64 bit size (root-io-spec ERRATA 4; #148)
        (0x02, _object64(100, 7)),
        (0x02, (100).to_bytes(8, "little") + (7).to_bytes(8, "little")),
        # A type no one uses yet, with no payload
        (0x40, b""),
    ],
)
def test_unknown_type_is_kept(locator_type: int, payload: bytes):
    data = _head(locator_type, len(payload), reserved=3) + payload
    locator, rest = _read(data + b"next")
    assert locator == UnknownLocator(locator_type, 3, payload)
    assert bytes(rest.data) == b"next"


@pytest.mark.parametrize("cls", [LargeLocator, UnknownLocator])
def test_only_rlocator_reads_a_locator(cls: type[RLocator]):
    """Which class a locator is depends on its first word: LargeLocator.read on
    a standard locator would give a StandardLocator"""
    data = (12).to_bytes(4, "little") + (550).to_bytes(8, "little")
    assert _read(data)[0] == StandardLocator(12, 550)
    buffer = ReadBuffer(memoryview(data), 0, BOOTSTRAP_CONTEXT, BufferContext(abspos=0))
    with pytest.raises(TypeError, match=f"not {cls.__name__}.read"):
        cls.read(buffer)


def test_locator_shorter_than_its_header_raises():
    with pytest.raises(ValueError, match="less than its own 4-byte header"):
        _read(_head(0x7E, -2))


def test_unknown_locator_cannot_be_fetched():
    unknown = UnknownLocator(0x7E, 0, _object64(0, 0))
    with pytest.raises(NotImplementedError, match="not in the file"):
        _ = RPageDescription(-3, unknown).page_locator
    with pytest.raises(NotImplementedError, match="not in the file"):
        REnvelopeLink(100, unknown).envelope_locator(PageListEnvelope)


@pytest.mark.skipif(not ANCHOR.exists(), reason="root-io-spec not checked out")
def test_page_list_with_an_unknown_locator_reads():
    """rntuple/anchor.root's page list (632-796, stored raw) with column 0's page
    locator (12 bytes at 724: size 12, offset 550) replaced by a type 0x7e
    locator of the same length, and the envelope checksum recomputed. The page
    list and the RNTuple still read; only that page's locator raises."""
    raw = bytearray(ANCHOR.read_bytes())
    standard = (12).to_bytes(4, "little") + (550).to_bytes(8, "little")
    assert raw.count(standard) == 1
    assert raw.index(standard) == 724
    payload = b"\x01\x02\x03\x04\x05\x06\x07\x08"
    raw[724:736] = _head(0x7E, len(payload)) + payload
    raw[796 - 8 : 796] = xxhash.xxh3_64_intdigest(bytes(raw[632 : 796 - 8])).to_bytes(
        8, "little"
    )
    data = bytes(raw)

    def read_at(offset: int, size: int) -> bytes:
        return data[offset : offset + size]

    fetch = Fetcher(read_at, BOOTSTRAP_CONTEXT)
    file = fetch(InitialReadLocator())
    tfile = fetch.resolve(file.tfile_locator)
    keylist = fetch.resolve(tfile.rootdir.keylist_locator)
    (key,) = [k for k in keylist.values() if k.fClassName == b"ROOT::RNTuple"]
    anchor = fetch(key)
    assert isinstance(anchor, ROOT3a3aRNTuple)
    rntuple = fetch.rntuple(anchor)
    (pagelist,) = rntuple.pagelistEnvelopes
    pages = [page for column in pagelist.pageLocations[0] for page in column]
    unknown = [p for p in pages if isinstance(p.locator, UnknownLocator)]
    assert [p.locator for p in unknown] == [UnknownLocator(0x7E, 0, payload)]
    assert unknown[0].fNElements == -3
    with pytest.raises(NotImplementedError, match="not in the file"):
        _ = unknown[0].page_locator
    for page in pages:
        if page is not unknown[0]:
            assert isinstance(page.page_locator.locator, StandardLocator)
    # The view of the clusters still builds: it uses the descriptions only
    (cluster,) = rntuple.clusters()
    assert cluster.columnRanges[0].pages[0].pageDescription is unknown[0]
