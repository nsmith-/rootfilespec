from pathlib import Path
from typing import Annotated, Any

import pytest

from rootfilespec.bootstrap import BOOTSTRAP_CONTEXT, ROOT3a3aRNTuple, TString
from rootfilespec.bootstrap.TFile import InitialReadLocator
from rootfilespec.bootstrap.TKey import TypedTKey
from rootfilespec.bootstrap.TStreamerInfo import TStreamerInfo
from rootfilespec.container import StdVector
from rootfilespec.dynamic import streamerinfo_to_classes
from rootfilespec.reader import Fetcher, open_path
from rootfilespec.serializable import (
    BufferContext,
    ReadBuffer,
    ROOTSerializable,
    serializable,
)
from rootfilespec.structutil import ROOTString, StringEncoding

DATA = Path(__file__).parent.parent / "reference" / "root-io-spec" / "data"

pytestmark = pytest.mark.skipif(
    not DATA.exists(), reason="reference/root-io-spec not checked out"
)


def _buffer(data: bytes, abspos: int = 0) -> ReadBuffer:
    return ReadBuffer(
        memoryview(data), 0, BOOTSTRAP_CONTEXT, BufferContext(abspos=abspos)
    )


def _read(
    encoding: StringEncoding, data: bytes, framed: bool = False
) -> tuple[bytes, bytes]:
    value, rest = ROOTString(encoding, framed).read(_buffer(data))
    return value, bytes(rest.data)


@serializable
class _Named(ROOTSerializable):
    name: Annotated[bytes, ROOTString("RNTuple")]
    after: Annotated[bytes, ROOTString("RNTuple")]


def test_rntuple_string_is_plain_bytes():
    """Issue #68: a string is read as the bytes on disk, not decoded"""
    name = "héllo·wörld".encode()
    data = len(name).to_bytes(4, "little") + name + b"\x00\x00\x00\x00" + b"rest"
    obj, rest = _Named.read(_buffer(data))
    assert obj.name == name
    assert type(obj.name) is bytes
    assert obj.after == b""
    assert bytes(rest.data) == b"rest"


def test_string_too_short():
    with pytest.raises(Exception):  # noqa: B017, PT011
        _Named.read(_buffer(b"\x09\x00\x00\x00abc"))


@pytest.mark.parametrize(
    ("data", "value"),
    [
        (b"\x00", b""),
        (b"\x05hello", b"hello"),
        (b"\xfe" + b"x" * 254, b"x" * 254),
        # a 255-character string is always in the long form (Conventions §5.1)
        (b"\xff\x00\x00\x00\xff" + b"y" * 255, b"y" * 255),
        (b"\xff\x00\x00\x01\x2c" + b"z" * 300, b"z" * 300),
    ],
)
def test_tstring_encoding(data: bytes, value: bytes):
    """Conventions §5.1: one length byte, or 255 then a 4-byte length"""
    assert _read("TString", data + b"rest") == (value, b"rest")


@pytest.mark.parametrize(
    ("data", "value"),
    [
        (b"\x00\x00\x00\x02hi", b"hi"),
        # no 255 escape: a length byte of 0xff is part of the i32
        (b"\x00\x00\x00\xff" + b"w" * 255, b"w" * 255),
        # a null pointer and an empty string are both four zero bytes
        (b"\x00\x00\x00\x00", b""),
        # n <= 0 means nothing follows
        (b"\xff\xff\xff\xff", b""),
    ],
)
def test_charstar_encoding(data: bytes, value: bytes):
    """Conventions §5.4 and #20: an i32 length, then that many bytes"""
    assert _read("charstar", data + b"rest") == (value, b"rest")


def test_framed_string():
    """Conventions §5.3: a std::string data member is a counted string inside a
    byte count and version word"""
    assert _read("TString", b"\x40\x00\x00\x06\x00\x0a\x03abcrest", framed=True) == (
        b"abc",
        b"rest",
    )
    # an empty std::string member is 7 bytes, not 1
    assert _read("TString", b"\x40\x00\x00\x03\x00\x0a\x00", framed=True) == (b"", b"")
    with pytest.raises(ValueError, match="Expected a byte count and version"):
        _read("TString", b"\x00\x00\x00\x06\x00\x0a\x03abc", framed=True)
    with pytest.raises(ValueError, match="does not match"):
        _read("TString", b"\x40\x00\x00\x07\x00\x0a\x03abcd", framed=True)


def test_rntuple_strings_are_bytes():
    """Every RNTuple string member of every root-io-spec RNTuple fixture is bytes"""
    for path in sorted((DATA / "rntuple").glob("*.root")):
        with open_path(path) as reader:
            for key in reader.keylist().values():
                if key.fClassName != b"ROOT::RNTuple":
                    continue
                anchor = reader.fetch(key)
                assert isinstance(anchor, ROOT3a3aRNTuple)
                rntuple = reader.fetch.rntuple(anchor)
                header = rntuple.headerEnvelope
                assert type(header.fName) is bytes
                assert header.fName == key.fName
                assert type(header.fLibrary) is bytes
                for field in rntuple.schemaDescription.fieldDescriptions:
                    for value in (
                        field.fFieldName,
                        field.fTypeName,
                        field.fTypeAlias,
                        field.fFieldDescription,
                    ):
                        assert type(value) is bytes


def test_key_and_named_strings_are_bytes():
    """TKey names and TNamed members are bytes: `container/file-minimal` holds a
    TObjString named `s` whose string is `hello` (Conventions §5.1)"""
    with open_path(DATA / "container" / "file-minimal.root") as reader:
        keylist = reader.keylist()
        assert all(type(name) is bytes for name, _ in keylist)
        for key in keylist.values():
            assert type(key.fClassName) is bytes
            assert type(key.fTitle) is bytes
        infos = reader.streamerinfos()
        assert all(type(name) is bytes for name in infos)
        assert all(type(info.fTitle) is bytes for info in infos.values())


def _streamerinfo(path: Path, classname: bytes) -> TStreamerInfo:
    """A class's TStreamerInfo, read with the bootstrap types only, for a file
    whose other classes cannot all be generated yet"""
    raw = path.read_bytes()
    fetch = Fetcher(lambda offset, size: raw[offset : offset + size], BOOTSTRAP_CONTEXT)
    file = fetch(InitialReadLocator())
    assert file.streamerinfo_locator is not None
    streamerinfo = fetch.resolve(file.streamerinfo_locator)
    (info,) = (
        item
        for item in streamerinfo.items
        if isinstance(item, TStreamerInfo) and item.fName == classname
    )
    return info


def test_generated_string_members():
    """Generated classes annotate string members with ROOTString: a std::string
    member is framed, a vector<string>'s elements are bare (root-io-spec
    serialization/collections), and a char* member is kCharStar
    (serialization/element-types)"""
    path = DATA / "serialization" / "collections.root"
    with open_path(path) as reader:
        assert reader.streamerinfo is not None
        code = streamerinfo_to_classes(reader.streamerinfo)
    assert "fStr: Annotated[bytes, ROOTString('TString', framed=True)]" in code
    assert "fWords: StdVector[Annotated[bytes, ROOTString('TString')]]" in code

    info = _streamerinfo(DATA / "serialization" / "element-types.root", b"ElementZoo")
    for name in (b"fText", b"fNull"):
        mdef, deps = info.element(name).member_definition(parent=info)
        assert mdef == f"{name.decode()}: Annotated[bytes, ROOTString('charstar')]"
        assert deps == []


@serializable
class _CollectionStrings(ROOTSerializable):
    fWords: StdVector[TString]


@serializable
class _StdStringMember(ROOTSerializable):
    fStr: Annotated[bytes, ROOTString("TString", framed=True)]


def test_fixture_string_bytes():
    """The string members of root-io-spec's uncompressed fixtures, read at the
    offsets their case.toml pins"""
    raw = (DATA / "serialization" / "collections.root").read_bytes()
    words, _ = _CollectionStrings.read(_buffer(raw[453:], 453))
    assert words.fWords.items == [b"pq", b"rs"]
    member, _ = _StdStringMember.read(_buffer(raw[501:], 501))
    assert member.fStr == b"abc"

    raw = (DATA / "serialization" / "element-types.root").read_bytes()
    text, rest = ROOTString("charstar").read(_buffer(raw[352:], 352))
    assert text == b"hi"
    null, _ = ROOTString("charstar").read(rest)
    assert null == b""


def test_string_records():
    """A TString, TStringLong or std::string stored as an object of its own reads
    as bytes: root-io-spec serialization/unframed-records and
    serialization/stringlong (Conventions §5.1, §5.1.1)"""
    with open_path(DATA / "serialization" / "unframed-records.root") as reader:
        keylist = reader.keylist()
        # A string record reads as bytes, so TKey.read_from is typed object (#135)
        assert reader.fetch(keylist.get_by_name(b"tstring")) == b"hello"
        assert reader.fetch(keylist.get_by_name(b"tstringlong")) == b"a long string"
    with open_path(DATA / "serialization" / "stringlong.root") as reader:
        keylist = reader.keylist()
        # Classes generated from the file's StreamerInfo have no static type
        small: Any = reader.fetch(keylist.get_by_name(b"small"))
        assert (small.fLong, small.fPlain) == (b"abcdef", b"abcdef")
        big: Any = reader.fetch(keylist.get_by_name(b"big"))
        assert (big.fLong, big.fPlain) == (b"x" * 300, b"x" * 300)


def test_typed_key_string_record():
    """A TypedTKey reads a string record too: the looked-up type is the
    TString alias, not a class, and read_from reads both kinds alike"""
    with open_path(DATA / "serialization" / "unframed-records.root") as reader:
        key = reader.keylist().get_by_name(b"tstring")
        buffer = reader.fetch.buffer(key)
        typed, _ = TypedTKey.read(buffer)
        assert typed.objtype == TString
        assert typed.read_from(buffer) == b"hello"
