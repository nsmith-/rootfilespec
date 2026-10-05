import struct
from pathlib import Path

import pytest
import xxhash  # type: ignore[import-not-found]

from rootfilespec.bootstrap import BOOTSTRAP_CONTEXT, ROOT3a3aRNTuple
from rootfilespec.reader import FileReader
from rootfilespec.rntuple.RFrame import ListFrame
from rootfilespec.rntuple.RNTuple import RNTuple
from rootfilespec.rntuple.schema import ExtraTypeInformation
from rootfilespec.serializable import BufferContext, ReadBuffer, read_value

DATA = Path(__file__).parent.parent / "reference" / "root-io-spec" / "data" / "rntuple"

# Issue #142: a frame's members are read from its fSize bytes only, as ROOT does
# (RNTupleSerialize.cxx:416-462), and members that overrun it raise an error
# naming the frame instead of "Cannot consume a negative number of bytes".

# An extra type information record: size, content ID 0, type version 0, type
# name "ab", and a content length of 0. 26 bytes.
RECORD = bytes.fromhex("1a00000000000000000000000000000002000000")
RECORD += b"ab" + bytes.fromhex("00000000")


def _buffer(data: bytes) -> ReadBuffer:
    return ReadBuffer(memoryview(data), 0, BOOTSTRAP_CONTEXT, BufferContext(abspos=0))


def _record_of_size(fSize: int) -> bytes:
    return struct.pack("<q", fSize) + RECORD[8:]


def test_valid_record_and_list():
    """The crafted frames read when their sizes are right; bytes the members
    don't use are kept"""
    record, rest = ExtraTypeInformation.read(_buffer(RECORD + b"next"))
    assert (record.fTypeName, record.fContent, record._unknown) == (b"ab", b"", b"")
    assert rest.data.tobytes() == b"next"

    record, _ = ExtraTypeInformation.read(_buffer(_record_of_size(28) + b"xy"))
    assert record._unknown == b"xy"

    data = struct.pack("<qI", -(12 + 2 * 26), 2) + 2 * RECORD
    frame, rest = read_value(ListFrame[ExtraTypeInformation], _buffer(data + b"next"))
    assert len(frame) == 2
    assert rest.data.tobytes() == b"next"


@pytest.mark.parametrize(
    "data",
    [
        # #142's two examples: a 22-byte record whose content length is outside
        # it, followed by the next bytes or by nothing
        _record_of_size(22)[:22] + bytes.fromhex("10000000") + b"0123456789abcdef",
        _record_of_size(22)[:22] + bytes.fromhex("10000000"),
    ],
    ids=["next-bytes", "end-of-buffer"],
)
def test_record_members_overrun(data: bytes):
    match = "ExtraTypeInformation: the members need more than the record frame's size of 22 bytes"
    with pytest.raises(ValueError, match=match):
        ExtraTypeInformation.read(_buffer(data))


def test_list_members_overrun():
    """A list frame of two items whose size only covers one"""
    data = struct.pack("<qI", -(12 + 26), 2) + 2 * RECORD
    match = "ListFrame: the members need more than the list frame's size of 38 bytes"
    with pytest.raises(ValueError, match=match):
        read_value(ListFrame[ExtraTypeInformation], _buffer(data))


def test_size_smaller_than_preamble():
    with pytest.raises(
        ValueError, match="record frame size 4 is smaller than its 8-byte preamble"
    ):
        ExtraTypeInformation.read(_buffer(_record_of_size(4)))
    data = struct.pack("<qI", -10, 0) + RECORD
    with pytest.raises(
        ValueError, match="list frame size 10 is smaller than its 12-byte preamble"
    ):
        read_value(ListFrame[ExtraTypeInformation], _buffer(data))


def test_size_past_the_buffer():
    with pytest.raises(
        ValueError, match="record frame size 27 runs past the 26 bytes left"
    ):
        ExtraTypeInformation.read(_buffer(_record_of_size(27)))
    data = struct.pack("<qI", -(12 + 27), 1) + RECORD
    with pytest.raises(
        ValueError, match="list frame size 39 runs past the 38 bytes left"
    ):
        read_value(ListFrame[ExtraTypeInformation], _buffer(data))


# rntuple/anchor.root is uncompressed; its case.toml pins the header envelope
# (preamble at 268, checksum at 500), the field list frame (-111 at 313: two
# fields of 46 and 53 bytes), the footer envelope (preamble at 838, length 160)
# and the footer's schema extension, a record frame of 56 at 862 holding four
# empty list frames.
HEADER = (268, 240)
FOOTER = (838, 160)


def _read_anchor(envelope: tuple[int, int], offset: int, fSize: int) -> RNTuple:
    """anchor.root with the frame size at ``offset`` replaced, and the
    envelope's checksum recomputed so that only the size differs"""
    data = bytearray((DATA / "anchor.root").read_bytes())
    struct.pack_into("<q", data, offset, fSize)
    start, length = envelope
    end = start + length - 8
    struct.pack_into("<Q", data, end, xxhash.xxh3_64_intdigest(data[start:end]))

    reader = FileReader.open(lambda offset, size: bytes(data[offset : offset + size]))
    (key,) = [k for k in reader.keylist().values() if k.fClassName == b"ROOT::RNTuple"]
    anchor = reader.fetch(key)
    assert isinstance(anchor, ROOT3a3aRNTuple)
    return reader.fetch.rntuple(anchor)


@pytest.mark.skipif(not DATA.exists(), reason="reference/root-io-spec not checked out")
def test_anchor_unchanged():
    """The patching itself changes nothing"""
    rntuple = _read_anchor(FOOTER, 862, 56)
    assert len(rntuple.headerEnvelope.fieldDescriptions) == 2


@pytest.mark.skipif(not DATA.exists(), reason="reference/root-io-spec not checked out")
def test_anchor_record_frame_too_small():
    """The schema extension's size covers three of its four list frames"""
    match = "SchemaExtension: the members need more than the record frame's size of 44 bytes"
    with pytest.raises(ValueError, match=match):
        _read_anchor(FOOTER, 862, 44)


@pytest.mark.skipif(not DATA.exists(), reason="reference/root-io-spec not checked out")
def test_anchor_list_frame_too_small():
    """The field list frame's size covers the first of its two fields"""
    match = "ListFrame fieldDescriptions: the members need more than the list frame's size of 58 bytes"
    with pytest.raises(ValueError, match=match):
        _read_anchor(HEADER, 313, -(12 + 46))
