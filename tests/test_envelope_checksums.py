from pathlib import Path

import pytest

from rootfilespec.bootstrap import BOOTSTRAP_CONTEXT, ROOT3a3aRNTuple
from rootfilespec.reader import open_path
from rootfilespec.rntuple.envelope import REnvelopeLocator
from rootfilespec.rntuple.header import HeaderEnvelope
from rootfilespec.rntuple.RLocator import StandardLocator
from rootfilespec.serializable import BufferContext, ReadBuffer

DATA = Path(__file__).parent.parent / "reference" / "root-io-spec" / "data" / "rntuple"
pytestmark = pytest.mark.skipif(
    not DATA.exists(), reason="reference/root-io-spec not checked out"
)


def _anchors(path: Path) -> list[ROOT3a3aRNTuple]:
    with open_path(path) as reader:
        keylist = reader.keylist()
        return [
            reader.fetch(keylist[name])
            for name in keylist
            if keylist[name].fClassName.fString == b"ROOT::RNTuple"
        ]


def _buffer(raw: bytes, offset: int, size: int) -> ReadBuffer:
    return ReadBuffer(
        memoryview(raw[offset : offset + size]),
        0,
        BOOTSTRAP_CONTEXT,
        BufferContext(abspos=offset),
    )


def _read(raw: bytes, loc):
    return loc.read_from(_buffer(raw, loc.offset, loc.size))


@pytest.mark.parametrize("name", sorted(p.name for p in DATA.glob("*.root")))
def test_every_envelope_verifies(name: str):
    """Issue #117: every envelope of every root-io-spec RNTuple fixture is checked

    Including rntuple/compressed.root, whose envelopes are compressed: the
    checksum is over the uncompressed envelope.
    """
    path = DATA / name
    raw = path.read_bytes()
    for anchor in _anchors(path):
        header = _read(raw, anchor.header_locator)
        footer = _read(raw, anchor.footer_locator)
        assert footer.headerChecksum == header.checksum
        for loc in footer.pagelist_locators:
            assert _read(raw, loc).headerChecksum == header.checksum


def test_corrupted_header_envelope_raises():
    """rntuple/anchor.root's header envelope is 268..508 (length 240, checksum at
    500..508). Changing any byte before the checksum must be caught."""
    path = DATA / "anchor.root"
    raw = bytearray(path.read_bytes())
    (anchor,) = _anchors(path)
    loc = anchor.header_locator
    assert (loc.offset, loc.size, loc.length) == (268, 240, 240)
    # The "n" of the ntuple's name "ntpl" at 288: flipping its case keeps the
    # envelope parseable, so only the checksum can catch it
    assert raw[288:292] == b"ntpl"
    raw[288] ^= 0x20
    with pytest.raises(ValueError, match="HeaderEnvelope checksum mismatch"):
        _read(bytes(raw), loc)


def test_corrupted_checksum_raises():
    path = DATA / "anchor.root"
    raw = bytearray(path.read_bytes())
    (anchor,) = _anchors(path)
    loc = anchor.header_locator
    raw[loc.offset + loc.length - 1] ^= 0x01
    with pytest.raises(ValueError, match="HeaderEnvelope checksum mismatch"):
        _read(bytes(raw), loc)


def test_stored_size_larger_than_length_raises():
    """root-io-spec NOTES 2: RNTuple decompression tests equality; a stored size
    larger than the uncompressed length is an error, not a raw payload"""
    loc = REnvelopeLocator(
        length=16, locator=StandardLocator(size=24, offset=0), envtype=HeaderEnvelope
    )
    with pytest.raises(ValueError, match="larger than its uncompressed length"):
        loc.read_from(_buffer(bytes(24), 0, 24))
