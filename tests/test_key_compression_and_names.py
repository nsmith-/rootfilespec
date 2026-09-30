import struct

import pytest

from rootfilespec.bootstrap import BOOTSTRAP_CONTEXT
from rootfilespec.bootstrap.strings import TString
from rootfilespec.bootstrap.TDirectory import TKeyList
from rootfilespec.bootstrap.TKey import TKey, TKey_header
from rootfilespec.serializable import BufferContext, ReadBuffer


def _tstring(s: bytes) -> bytes:
    return bytes([len(s)]) + s


def _key_bytes(
    classname: bytes, name: bytes, payload_len: int, objlen: int, seek: int = 100
) -> bytes:
    """A short (version 4) key record header, as TKey::Streamer writes it"""
    strings = _tstring(classname) + _tstring(name) + _tstring(b"")
    keylen = 4 + 2 + 4 + 4 + 2 + 2 + 4 + 4 + len(strings)
    return (
        struct.pack(">ihiIhhii", keylen + payload_len, 4, objlen, 0, keylen, 1, seek, 0)
        + strings
    )


def _buffer(data: bytes, abspos: int = 0) -> ReadBuffer:
    return ReadBuffer(
        memoryview(data), 0, BOOTSTRAP_CONTEXT, BufferContext(abspos=abspos)
    )


def _key(classname: bytes, name: bytes, payload: bytes, objlen: int) -> tuple:
    record = _key_bytes(classname, name, len(payload), objlen) + payload
    key, _ = TKey.read(_buffer(record))
    return key, record


def test_raw_payload_longer_than_objlen():
    """Issue #77: a payload longer than fObjLen is raw with slack, not compressed

    A TString of 4 bytes stored raw in a 6-byte payload. Treating it as
    compressed ("!=") read the slack as a compression block header.
    """
    key, record = _key(b"TString", b"s", b"\x03abc\x00\x00", objlen=4)
    assert not key.header.is_compressed()
    obj = key.read_object(lambda _seek, _size: _buffer(record, 100), TString)
    assert obj == TString(b"abc")


def test_compressed_is_objlen_larger_than_payload():
    header = TKey_header(
        fNbytes=60, fVersion=4, fObjlen=100, fDatime=0, fKeylen=40, fCycle=1
    )
    assert header.is_compressed()
    header = TKey_header(
        fNbytes=60, fVersion=4, fObjlen=20, fDatime=0, fKeylen=40, fCycle=1
    )
    assert not header.is_compressed()


def test_rblob_key_is_refused():
    """root-io-spec NOTES 1: an RBlob's fObjLen is decorative, so its payload
    cannot be read through the key"""
    key, record = _key(b"RBlob", b"", b"\x00" * 34, objlen=26)
    with pytest.raises(ValueError, match="is an RBlob"):
        key.read_object(lambda _seek, _size: _buffer(record, 100))


@pytest.mark.parametrize(("version", "short"), [(4, True), (1000, True), (1004, False)])
def test_large_key_threshold(version: int, short: bool):
    """Issue #87: a key is large when fVersion > 1000, as in TKey.cxx"""
    header = TKey_header(
        fNbytes=0, fVersion=version, fObjlen=0, fDatime=0, fKeylen=0, fCycle=1
    )
    assert header.is_short() is short


def test_non_ascii_key_names():
    """Issue #87: key names are bytes; any name can be listed and looked up"""
    utf8, _ = _key(b"TString", "café".encode(), b"\x00", objlen=1)
    other, _ = _key(b"TString", b"\xffname", b"\x00", objlen=1)
    keylist = TKeyList(fKeys=[utf8, other], padding=b"")
    names = list(keylist)
    assert names == ["café", "\udcffname"]
    assert keylist["café"] is utf8
    assert keylist[names[1]] is other
    assert names[1].encode("utf-8", "surrogateescape") == b"\xffname"
