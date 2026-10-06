"""The RNTuple anchor's checksum, and anchors of other lengths (#85, #98)

ROOT reads an anchor from the whole payload of its key: the ROOT::RNTuple
object, then an XXH3-64 checksum in the payload's last 8 bytes, outside the
object's byte count, over the object after its byte count and class version
(root-io-spec RNTuple ERRATA 2 and 3; RMiniFileReader::GetNTupleProperAtOffset,
tree/ntuple/src/RMiniFile.cxx:825-858 at 6.40.04). A later class version may
append fields, so the payload may be longer; ROOT refuses one shorter than
class version 2's 78 bytes (RMiniFile.cxx:812-818).
"""

import zlib
from pathlib import Path

import pytest
import xxhash  # type: ignore[import-not-found]

from rootfilespec.bootstrap import BOOTSTRAP_CONTEXT, ROOT3a3aRNTuple
from rootfilespec.bootstrap.TFile import InitialReadLocator
from rootfilespec.bootstrap.TKey import TKey
from rootfilespec.reader import Fetcher
from rootfilespec.serializable import BufferContext, ReadBuffer

DATA = Path(__file__).parent.parent / "reference" / "root-io-spec" / "data" / "rntuple"
pytestmark = pytest.mark.skipif(
    not DATA.exists(), reason="reference/root-io-spec not checked out"
)

# rntuple/anchor.root, from its case.toml: the anchor record at 998, its
# payload 1052..1130, the byte count at 1052, Version Epoch at 1058 and the
# checksum at 1122
KEY, PAYLOAD, END = 998, 1052, 1130
CHECKSUM = 0x7DD5F110D74D8756


def _fetcher(data: bytes) -> Fetcher:
    def read_at(offset: int, size: int) -> bytes:
        return data[offset : offset + size]

    return Fetcher(read_at, BOOTSTRAP_CONTEXT)


def _anchor_key(fetch: Fetcher) -> TKey:
    file = fetch(InitialReadLocator())
    tfile = fetch.resolve(file.tfile_locator)
    keylist = fetch.resolve(tfile.rootdir.keylist_locator)
    (key,) = [k for k in keylist.values() if k.fClassName == b"ROOT::RNTuple"]
    assert isinstance(key, TKey)
    return key


def _payload(fields: bytes, version: int = 2) -> bytes:
    """An anchor payload as ROOT writes it: the byte count, the class version,
    the fields, and the checksum of the fields"""
    obj = version.to_bytes(2, "big") + fields
    body = (0x40000000 | len(obj)).to_bytes(4, "big") + obj
    checksum: int = xxhash.xxh3_64_intdigest(body[6:])
    return body + checksum.to_bytes(8, "big")


def _read_through_key(payload: bytes) -> object:
    """Read a payload through anchor.root's anchor key, its fNbytes and fObjLen
    set to the payload's length"""
    raw = (DATA / "anchor.root").read_bytes()
    header = bytearray(raw[KEY:PAYLOAD])
    header[0:4] = (len(header) + len(payload)).to_bytes(4, "big")
    header[6:10] = len(payload).to_bytes(4, "big")
    buffer = ReadBuffer(
        memoryview(bytes(header) + payload),
        0,
        BOOTSTRAP_CONTEXT,
        BufferContext(abspos=KEY),
    )
    key, _ = TKey.read(buffer)
    return key.read_from(buffer)


def _fields() -> bytes:
    """anchor.root's anchor fields, Version Epoch to Max Key Size"""
    return (DATA / "anchor.root").read_bytes()[PAYLOAD + 6 : END - 8]


def test_checksum():
    """The case.toml's bytes: the checksum at 1122 is xxh3_64 of 1058..1122,
    stored big-endian, outside the byte count of 66 at 1052 (ERRATA 3)"""
    raw = (DATA / "anchor.root").read_bytes()
    assert raw[PAYLOAD : PAYLOAD + 6] == bytes.fromhex("400000420002")
    assert raw[END - 8 : END] == bytes.fromhex("7dd5f110d74d8756")
    assert xxhash.xxh3_64_intdigest(raw[PAYLOAD + 6 : END - 8]) == CHECKSUM

    fetch = _fetcher(raw)
    key = _anchor_key(fetch)
    assert (key.fSeekKey, key.header.fKeylen, key.header.fObjlen) == (KEY, 54, 78)
    anchor = fetch(key)
    assert isinstance(anchor, ROOT3a3aRNTuple)
    assert anchor.checksum == CHECKSUM
    assert anchor.fVersionClass == 2
    assert anchor._unknown == b""
    assert (anchor.fVersionEpoch, anchor.fSeekHeader, anchor.fMaxKeySize) == (
        1,
        268,
        1073741824,
    )


@pytest.mark.parametrize(
    "offset",
    [
        PAYLOAD + 6,  # Version Epoch's first byte: the first the checksum covers
        END - 9,  # Max Key Size's last byte
        END - 1,  # the checksum's last byte
    ],
)
def test_corrupted_byte_raises(offset: int):
    raw = bytearray((DATA / "anchor.root").read_bytes())
    raw[offset] ^= 0x01
    fetch = _fetcher(bytes(raw))
    with pytest.raises(ValueError, match="RNTuple anchor checksum mismatch"):
        fetch(_anchor_key(fetch))


def test_compressed_anchor():
    """rntuple/compressed.root's anchor key at 727 (the case.toml's records)
    stores the 78 bytes in a zlib block: the checksum is over the decompressed
    payload, taken here with zlib alone"""
    raw = (DATA / "compressed.root").read_bytes()
    fetch = _fetcher(raw)
    key = _anchor_key(fetch)
    assert (key.fSeekKey, key.header.fNbytes, key.header.fObjlen) == (727, 113, 78)
    assert key.header.is_compressed()
    stored = raw[key.fSeekKey + key.header.fKeylen : key.fSeekKey + key.header.fNbytes]
    assert stored[:2] == b"ZL"
    payload = zlib.decompress(stored[9:])
    assert len(payload) == 78
    assert payload[:6] == bytes.fromhex("400000420002")

    anchor = fetch(key)
    assert isinstance(anchor, ROOT3a3aRNTuple)
    assert anchor.checksum == int.from_bytes(payload[70:], "big")
    assert anchor.checksum == xxhash.xxh3_64_intdigest(payload[6:70])


def test_through_a_key():
    """The helper rebuilds anchor.root's anchor exactly"""
    payload = _payload(_fields())
    assert payload == (DATA / "anchor.root").read_bytes()[PAYLOAD:END]
    anchor = _read_through_key(payload)
    assert isinstance(anchor, ROOT3a3aRNTuple)
    assert anchor.checksum == CHECKSUM


def test_longer_anchor_keeps_its_tail():
    """A later class version that appends 8 bytes of fields: ROOT reads the
    fields it knows and takes the checksum from the last 8 bytes"""
    tail = bytes(range(1, 9))
    anchor = _read_through_key(_payload(_fields() + tail, version=3))
    assert isinstance(anchor, ROOT3a3aRNTuple)
    assert anchor.fVersionClass == 3
    assert anchor._unknown == tail
    assert anchor.fSeekHeader == 268
    assert anchor.fMaxKeySize == 1073741824


def test_later_class_version():
    anchor = _read_through_key(_payload(_fields(), version=3))
    assert isinstance(anchor, ROOT3a3aRNTuple)
    assert anchor.fVersionClass == 3
    assert anchor._unknown == b""


@pytest.mark.parametrize("size", [70, 77])
def test_short_anchor_raises(size: int):
    """70 bytes is class version 2 without its checksum"""
    payload = _payload(_fields())[:size]
    with pytest.raises(ValueError, match=f"RNTuple anchor of {size} bytes"):
        _read_through_key(payload)


@pytest.mark.parametrize("version", [0, 1])
def test_earlier_class_version_raises(version: int):
    payload = _payload(_fields(), version=version)
    with pytest.raises(ValueError, match=f"class version {version}: the first is 2"):
        _read_through_key(payload)


@pytest.mark.parametrize("epoch", [0, 2])
def test_other_epoch_raises(epoch: int):
    fields = epoch.to_bytes(2, "big") + _fields()[2:]
    with pytest.raises(NotImplementedError, match=f"format version {epoch}.0.2.0"):
        _read_through_key(_payload(fields))


def test_bytes_outside_the_byte_count_raise():
    """8 bytes between the object, as its byte count of 66 gives it, and the
    checksum: ROOT's two readers would look for the checksum in different places"""
    body = _payload(_fields())[:-8] + bytes(8)
    payload = body + xxhash.xxh3_64_intdigest(body[6:]).to_bytes(8, "big")
    with pytest.raises(ValueError, match="does not span the 78 bytes"):
        _read_through_key(payload)
