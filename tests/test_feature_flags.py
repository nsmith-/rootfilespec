"""The RNTuple feature flags: a reader refuses any flag it does not implement (#121)

A reader must refuse an RNTuple with an unknown feature flag (spec, *Notes on
Backward and Forward Compatibility*; root-io-spec RNTuple NOTES 6). Spec v1.0.2.1
defines flag 0, *Nested Deferred Columns*, which rootfilespec does not implement,
and ROOT 6.40.04 refuses it too (RNTupleSerialize.cxx:1869-1877).
"""

import struct
from pathlib import Path

import pytest
import xxhash  # type: ignore[import-not-found]

from rootfilespec.bootstrap import ROOT3a3aRNTuple
from rootfilespec.reader import FileReader
from rootfilespec.rntuple.RNTuple import RNTuple

DATA = Path(__file__).parent.parent / "reference" / "root-io-spec" / "data" / "rntuple"
pytestmark = pytest.mark.skipif(
    not DATA.exists(), reason="reference/root-io-spec not checked out"
)

# anchor.root is uncompressed. From its case.toml: the header envelope is at 268,
# 240 bytes long, its feature flags at 276; the footer envelope is at 838, 160
# bytes long, its feature flags at 846. Each envelope ends with its checksum.
HEADER, FOOTER = (268, 240, 276), (838, 160, 846)


def _read(envelope: tuple[int, int, int], flags: int) -> RNTuple:
    """anchor.root with an envelope's feature flags set to ``flags``, and its
    checksum recomputed so that only the flags differ"""
    start, length, at = envelope
    data = bytearray((DATA / "anchor.root").read_bytes())
    assert struct.unpack_from("<q", data, at) == (0,)
    struct.pack_into("<q", data, at, flags)
    end = start + length - 8
    struct.pack_into("<Q", data, end, xxhash.xxh3_64_intdigest(data[start:end]))

    reader = FileReader.open(lambda offset, size: bytes(data[offset : offset + size]))
    (key,) = [k for k in reader.keylist().values() if k.fClassName == b"ROOT::RNTuple"]
    anchor = reader.fetch(key)
    assert isinstance(anchor, ROOT3a3aRNTuple)
    return reader.fetch.rntuple(anchor)


@pytest.mark.parametrize("envelope", [HEADER, FOOTER], ids=["header", "footer"])
def test_unchanged(envelope: tuple[int, int, int]):
    """The patching itself changes nothing: anchor.root reads as it is"""
    rntuple = _read(envelope, 0)
    assert rntuple.headerEnvelope.featureFlags.flags == 0
    assert rntuple.footerEnvelope.featureFlags.flags == 0


@pytest.mark.parametrize("envelope", [HEADER, FOOTER], ids=["header", "footer"])
def test_nested_deferred_columns_is_refused(envelope: tuple[int, int, int]):
    """Flag 0 is refused by name, in the header and in the footer"""
    with pytest.raises(
        NotImplementedError,
        match=r"feature flags 0x0000000000000001: flag 0 \(Nested Deferred Columns\)$",
    ):
        _read(envelope, 1)


@pytest.mark.parametrize(
    ("flags", "named"),
    [
        (0b11, r"flag 0 \(Nested Deferred Columns\), unknown flag 1$"),
        (1 << 62, "0x4000000000000000: unknown flag 62$"),
        # Bit 63 says that more flags follow, in a further word
        (-(1 << 63), r"0x8000000000000000: flags above 62 \(bit 63\)$"),
        (-(1 << 63) | 1 << 5, r"unknown flag 5, flags above 62 \(bit 63\)$"),
    ],
    ids=["flags 0 and 1", "flag 62", "bit 63", "flag 5 and bit 63"],
)
def test_unknown_flags_are_refused(flags: int, named: str):
    with pytest.raises(NotImplementedError, match=named):
        _read(HEADER, flags)
