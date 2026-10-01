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
            if keylist[name].fClassName == b"ROOT::RNTuple"
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


def test_header_checksum_is_the_pinned_value():
    """rntuple/anchor.root's case.toml pins the header envelope's checksum at 500
    and the footer's copy of it at 854"""
    path = DATA / "anchor.root"
    raw = path.read_bytes()
    (anchor,) = _anchors(path)
    pinned = bytes([0x2A, 0x13, 0xA2, 0x84, 0xB3, 0x59, 0xD3, 0xDD])
    assert raw[500:508] == raw[854:862] == pinned
    header = _read(raw, anchor.header_locator)
    footer = _read(raw, anchor.footer_locator)
    assert header.checksum == footer.headerChecksum == int.from_bytes(pinned, "little")


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


def test_envelope_shorter_than_16_bytes_raises():
    """The length counts the 8-byte preamble and the 8-byte checksum, so it is
    at least 16, as ROOT's DeserializeEnvelope requires"""
    loc = REnvelopeLocator(
        length=8, locator=StandardLocator(size=8, offset=0), envtype=HeaderEnvelope
    )
    # The preamble alone: type 1 in the low 16 bits, length 8 in the upper 48
    preamble = (8 << 16 | 0x01).to_bytes(8, "little")
    with pytest.raises(ValueError, match="shorter than 16 bytes"):
        loc.read_from(_buffer(preamble, 0, 8))


def test_envelope_length_not_matching_the_locator_raises():
    """rntuple/anchor.root's header envelope with the length in its preamble
    (bytes 270..276) changed from 240 to 248: the anchor says 240 were stored"""
    path = DATA / "anchor.root"
    raw = bytearray(path.read_bytes())
    (anchor,) = _anchors(path)
    loc = anchor.header_locator
    assert raw[268:276] == (240 << 16 | 0x01).to_bytes(8, "little")
    raw[268:276] = (248 << 16 | 0x01).to_bytes(8, "little")
    with pytest.raises(
        ValueError, match=r"Length of envelope \(248\) .* buffer length \(240\)"
    ):
        _read(bytes(raw), loc)


def test_every_corrupted_byte_is_a_checksum_mismatch():
    """The checksum is verified before the payload is parsed, as ROOT does, so
    corrupting any byte the checksum covers reports the checksum, not whatever
    the parser trips over first (an out-of-range slice, unknown feature flags,
    ...)."""
    path = DATA / "anchor.root"
    raw = path.read_bytes()
    (anchor,) = _anchors(path)
    loc = anchor.header_locator
    # The 8-byte preamble is checked first (type, length); everything after it,
    # up to the checksum, is covered only by the checksum
    for pos in range(loc.offset + 8, loc.offset + loc.length - 8):
        corrupted = bytearray(raw)
        corrupted[pos] ^= 0x80
        with pytest.raises(ValueError, match="HeaderEnvelope checksum mismatch"):
            _read(bytes(corrupted), loc)


@pytest.mark.parametrize("size", [0, 200, 239])
def test_short_read_raises(size: int):
    """Fewer bytes than the locator's size (a truncated file) are reported as
    such, not taken for a compressed envelope"""
    path = DATA / "anchor.root"
    raw = path.read_bytes()
    (anchor,) = _anchors(path)
    loc = anchor.header_locator
    with pytest.raises(ValueError, match=f"expected 240 bytes, got {size}"):
        loc.read_from(_buffer(raw, loc.offset, size))
