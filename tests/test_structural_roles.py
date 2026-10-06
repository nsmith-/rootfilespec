"""A field's structural role as the StructuralRole enum (spec, *Field Description*)

The stored value is kept, and a role the enum does not list still reads: a
reader skips such a field rather than refusing the RNTuple (spec, *Notes on
Backward and Forward Compatibility*), as ROOT does with ``kUnknown``.
"""

import struct
from pathlib import Path

import pytest
import xxhash  # type: ignore[import-not-found]

from rootfilespec.bootstrap import ROOT3a3aRNTuple
from rootfilespec.reader import FileReader, open_path
from rootfilespec.rntuple.RNTuple import RNTuple
from rootfilespec.rntuple.schema import StructuralRole

DATA = Path(__file__).parent.parent / "reference" / "root-io-spec" / "data" / "rntuple"
pytestmark = pytest.mark.skipif(
    not DATA.exists(), reason="reference/root-io-spec not checked out"
)


def _rntuple(reader: FileReader) -> RNTuple:
    (key,) = [k for k in reader.keylist().values() if k.fClassName == b"ROOT::RNTuple"]
    anchor = reader.fetch(key)
    assert isinstance(anchor, ROOT3a3aRNTuple)
    return reader.fetch.rntuple(anchor)


def _roles(name: str) -> dict[bytes, StructuralRole | None]:
    with open_path(DATA / name) as reader:
        fields = _rntuple(reader).schemaDescription.fieldDescriptions
    return {field.fFieldName: field.structural_role for field in fields}


def test_every_role_in_the_fixtures():
    """collections.root has the first four roles, streamed.root the fifth"""
    roles = _roles("collections.root")
    assert roles[b"fString"] is StructuralRole.kPlain
    assert roles[b"fVector"] is StructuralRole.kCollection
    assert roles[b"fPair"] is StructuralRole.kRecord
    assert roles[b"fVariant"] is StructuralRole.kVariant
    assert _roles("streamed.root")[b"fInner"] is StructuralRole.kStreamer


def test_unknown_role_reads():
    """anchor.root's header with field 0's role set to 0x05 (u16 at 345, its
    case.toml) and the checksum at 500 recomputed: it reads, the stored value is
    kept, and the role is None"""
    data = bytearray((DATA / "anchor.root").read_bytes())
    assert struct.unpack_from("<H", data, 345) == (0,)
    struct.pack_into("<H", data, 345, 0x05)
    struct.pack_into("<Q", data, 500, xxhash.xxh3_64_intdigest(data[268:500]))

    reader = FileReader.open(lambda offset, size: bytes(data[offset : offset + size]))
    (key,) = [k for k in reader.keylist().values() if k.fClassName == b"ROOT::RNTuple"]
    anchor = reader.fetch(key)
    assert isinstance(anchor, ROOT3a3aRNTuple)
    header = reader.fetch(anchor.header_locator)
    field = header.fieldDescriptions.items[0]
    assert field.fStructuralRole == 0x05
    assert field.structural_role is None
