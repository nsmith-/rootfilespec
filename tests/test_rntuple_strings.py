from pathlib import Path
from typing import Annotated

import pytest

from rootfilespec.bootstrap import BOOTSTRAP_CONTEXT
from rootfilespec.reader import open_path
from rootfilespec.rntuple.RNTuple import RNTuple
from rootfilespec.serializable import (
    BufferContext,
    ReadBuffer,
    ROOTSerializable,
    serializable,
)
from rootfilespec.structutil import CountedString

DATA = Path(__file__).parent.parent / "reference" / "root-io-spec" / "data" / "rntuple"


@serializable
class _Named(ROOTSerializable):
    name: Annotated[bytes, CountedString("<I")]
    after: Annotated[bytes, CountedString("<I")]


def _buffer(data: bytes) -> ReadBuffer:
    return ReadBuffer(memoryview(data), 0, BOOTSTRAP_CONTEXT, BufferContext(abspos=0))


def test_counted_string_is_plain_bytes():
    """Issue #68: a counted string is read as the bytes on disk, not decoded"""
    name = "héllo·wörld".encode()
    data = len(name).to_bytes(4, "little") + name + b"\x00\x00\x00\x00" + b"rest"
    obj, rest = _Named.read(_buffer(data))
    assert obj.name == name
    assert type(obj.name) is bytes
    assert obj.after == b""
    assert bytes(rest.data) == b"rest"


def test_counted_string_too_short():
    with pytest.raises(Exception):  # noqa: B017, PT011
        _Named.read(_buffer(b"\x09\x00\x00\x00abc"))


@pytest.mark.skipif(not DATA.exists(), reason="reference/root-io-spec not checked out")
def test_rntuple_strings_are_bytes():
    """Every RNTuple string member of every root-io-spec RNTuple fixture is bytes"""
    for path in sorted(DATA.glob("*.root")):
        with open_path(path) as reader:
            keylist = reader.keylist()
            for name in keylist:
                key = keylist[name]
                if key.fClassName.fString != b"ROOT::RNTuple":
                    continue
                rntuple = RNTuple.from_anchor(reader.fetch(key), reader.fetch.buffer)
                header = rntuple.headerEnvelope
                assert type(header.fName) is bytes
                assert header.fName == name.encode()
                assert type(header.fLibrary) is bytes
                for field in rntuple.schemaDescription.fieldDescriptions:
                    for value in (
                        field.fFieldName,
                        field.fTypeName,
                        field.fTypeAlias,
                        field.fFieldDescription,
                    ):
                        assert type(value) is bytes
