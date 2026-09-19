"""Check what rootfilespec reads against the case descriptions of root-io-spec

Next to each fixture of reference/root-io-spec, ``gen/cases/<id>/case.toml`` lists
the records of the file and asserts the value of individual fields by their
position in the file. Those assertions are about the file rather than about a
reader, so to use them we trace every primitive that rootfilespec reads, with
its position, and require that the two agree on where fields are.
"""

import re
import struct
from bisect import bisect_right
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
import tomli  # tomllib, once Python 3.10 is dropped

from rootfilespec.bootstrap.TKey import TKey
from rootfilespec.serializable import ReadBuffer
from tests.test_read import read_file
from tests.test_spec_fixtures import EXPECTED_FAILURES

SPEC = Path(__file__).parent.parent / "reference" / "root-io-spec"
CASES = sorted(
    path.parent.relative_to(SPEC / "gen" / "cases").as_posix()
    for path in (SPEC / "gen" / "cases").rglob("case.toml")
)

if not CASES:
    pytest.skip(
        "The reference/root-io-spec submodule is not checked out",
        allow_module_level=True,
    )

# The types of a [[bytes]] assertion, as in tools/check_bytes.py of root-io-spec
FORMATS = {
    "i8": ">b", "u8": ">B", "i16": ">h", "u16": ">H", "i32": ">i", "u32": ">I",
    "i64": ">q", "u64": ">Q", "f32": ">f", "f64": ">d",
    "i16le": "<h", "u16le": "<H", "i32le": "<i", "u32le": "<I",
    "i64le": "<q", "u64le": "<Q", "f32le": "<f", "f64le": "<d",
}  # fmt: skip

# Cases in which none of the asserted fields is read, although the fixture reads
UNTRACED = {
    # The assertions are on the key at the front of each record, which we do not
    # read: the copy of the key in the key list is used (see #81)
    "container/cycles",
    "container/directories",
    # The walker of test_read.py does not read these baskets (#113)
    "ttree/basket",
    "ttree/basket-compressed",
    "ttree/basket-multiblock",
}


@dataclass(frozen=True)
class Field:
    offset: int
    order: str
    "Byte order character of the struct format, @ if it had none"
    code: str
    value: Any

    @property
    def size(self) -> int:
        return struct.calcsize("=" + self.code)

    @property
    def stop(self) -> int:
        return self.offset + self.size

    @property
    def is_float(self) -> bool:
        return self.code in "efd"

    def __str__(self) -> str:
        return f"{self.order}{self.code} at {self.offset}"


@dataclass
class Trace:
    """What was read from the file, by position in the file

    Reads from decompressed data have no position in the file and are left out.
    """

    fields: dict[int, Field] = field(default_factory=dict)
    raw: set[tuple[int, int]] = field(default_factory=set)
    "Ranges read as bytes: strings, arrays and uninterpreted data"
    keys: dict[int, TKey] = field(default_factory=dict)
    "Keys by fSeekKey, wherever they were read from"

    def covering(self, offset: int) -> Field | None:
        """The field that starts before this offset and ends after it"""
        starts = sorted(self.fields)
        index = bisect_right(starts, offset) - 1
        if index < 0:
            return None
        found = self.fields[starts[index]]
        return found if found.offset < offset < found.stop else None


def _position(buffer: ReadBuffer) -> int | None:
    if buffer.context.abspos is None:
        return None
    return buffer.context.abspos + buffer.relpos


@pytest.fixture
def trace(monkeypatch: pytest.MonkeyPatch) -> Trace:
    out = Trace()
    unpack, consume, consume_view = (
        ReadBuffer.unpack,
        ReadBuffer.consume,
        ReadBuffer.consume_view,
    )
    read_key = TKey.read.__func__  # type: ignore[attr-defined]

    def traced_unpack(self: ReadBuffer, fmt: str):
        values, rest = unpack(self, fmt)
        position = _position(self)
        if position is not None:
            order, body = (fmt[0], fmt[1:]) if fmt[0] in "@=<>!" else ("@", fmt)
            remaining = list(values)
            for count, code in re.findall(r"(\d*)([a-zA-Z?])", body):
                if code in "sp":
                    out.raw.add((position, position + int(count or 1)))
                    position += int(count or 1)
                    remaining.pop(0)
                    continue
                for _ in range(int(count or 1)):
                    value = None if code == "x" else remaining.pop(0)
                    item = Field(position, order, code, value)
                    if code != "x":
                        out.fields[position] = item
                    position = item.stop
        return values, rest

    def traced_consume(self: ReadBuffer, size: int):
        position = _position(self)
        if position is not None and size > 0:
            out.raw.add((position, position + size))
        return consume(self, size)

    def traced_consume_view(self: ReadBuffer, size: int):
        position = _position(self)
        if position is not None and size > 0:
            out.raw.add((position, position + size))
        return consume_view(self, size)

    def traced_read_key(cls: type[TKey], buffer: ReadBuffer):
        key, rest = read_key(cls, buffer)
        out.keys[key.fSeekKey] = key
        return key, rest

    monkeypatch.setattr(ReadBuffer, "unpack", traced_unpack)
    monkeypatch.setattr(ReadBuffer, "consume", traced_consume)
    monkeypatch.setattr(ReadBuffer, "consume_view", traced_consume_view)
    monkeypatch.setattr(TKey, "read", classmethod(traced_read_key))
    return out


def _load(case: str) -> tuple[dict[str, Any], Path]:
    with (SPEC / "gen" / "cases" / case / "case.toml").open("rb") as file:
        description = tomli.load(file)
    fixture = Path(description["file"]).relative_to("data").as_posix()
    if fixture in EXPECTED_FAILURES:
        pytest.skip(f"{fixture} is an expected failure of test_spec_fixtures.py")
    return description, SPEC / description["file"]


def _same_order(found: Field, order: str) -> bool:
    return found.size == 1 or found.order == order


def _compare(assertion: dict[str, Any], trace: Trace, data: bytes) -> tuple[bool, str]:
    """Whether the asserted field was read as such, and what is wrong if it was
    read as something else. A field that was not read at all is neither."""
    offset: int = assertion["offset"]
    if assertion["type"] == "bytes":
        return offset in trace.fields or any(offset == a for a, _ in trace.raw), ""
    if assertion["type"] == "string":
        # A counted string: a length byte, and then as many bytes
        length = data[offset]
        found = trace.fields.get(offset)
        if found is None:
            inside = trace.covering(offset)
            return False, f"string length is inside {inside}" if inside else ""
        if found.size != 1 or found.value != length:
            return False, f"string length {length} was read as {found}"
        if length and (offset + 1, offset + 1 + length) not in trace.raw:
            return False, f"the {length} bytes of the string were not read as such"
        return True, ""

    fmt = FORMATS[assertion["type"]]
    order, size, is_float = fmt[0], struct.calcsize(fmt), fmt[1] in "fd"
    found = trace.fields.get(offset)
    if found is None:
        found = trace.covering(offset)
        if found is None:
            return False, ""
        # e.g. the low half of a 64 bit integer
        if found.is_float or is_float or not _same_order(found, order):
            return False, f"is inside {found}"
        return offset + size <= found.stop, ""
    if found.is_float != is_float:
        return False, f"was read as {found}"
    if found.size == size:
        if not _same_order(found, order):
            return False, f"was read as {found}: wrong or unspecified byte order"
        mask = (1 << (8 * size)) - 1
        same = (
            struct.pack(fmt, assertion["value"]) == struct.pack(fmt, found.value)
            if is_float
            else int(found.value) & mask == int(assertion["value"]) & mask
        )
        return same, "" if same else f"was read as {found} = {found.value!r}"
    if found.size > size:
        return _same_order(found, order), (
            "" if _same_order(found, order) else f"is the front of {found}"
        )
    # Read in parts, e.g. a class tag as two 16 bit integers: they must tile it
    position = offset
    while position < offset + size:
        part = trace.fields.get(position)
        if part is None or part.is_float or not _same_order(part, order):
            return False, f"was read in parts that stop making sense at {position}"
        position = part.stop
    return position == offset + size, (
        "" if position == offset + size else f"was read in parts that end at {position}"
    )


@pytest.mark.parametrize("case", CASES)
def test_case_bytes(case: str, trace: Trace):
    """The fields that case.toml asserts are fields to rootfilespec as well

    This does not require that everything is read, only that nothing is read as
    something else: a field at the same position has the same size and an
    explicit and equal byte order, and no asserted field starts in the middle of
    a field of another kind. That catches what reading "without an error" cannot,
    e.g. a native byte order (#92) or a misplaced frame that happens to parse.
    """
    description, path = _load(case)
    read_file(path)
    data = path.read_bytes()
    matched = 0
    problems: list[str] = []
    for assertion in description.get("bytes", []):
        ok, problem = _compare(assertion, trace, data)
        matched += ok
        if problem:
            name = assertion.get("name", "")
            problems.append(
                f"{assertion['type']} at {assertion['offset']} ({name}) {problem}"
            )
    assert not problems, "\n".join(problems)
    # Whether or not the case asserts them: nothing in a ROOT file is in the byte
    # order of the machine that reads it
    native = [
        item for item in trace.fields.values() if item.order in "@=" and item.size > 1
    ]
    assert not native, "Read without a byte order: " + ", ".join(map(str, native))
    # A reader that stops visiting a record would otherwise pass unnoticed
    assert (matched == 0) == (case in UNTRACED), f"{matched} asserted fields were read"


@pytest.mark.parametrize("case", CASES)
def test_case_records(case: str, trace: Trace):
    """The keys that rootfilespec reads agree with the records that case.toml lists"""
    description, path = _load(case)
    if "records" not in description:
        pytest.skip("The case does not list the records of the file")
    read_file(path)
    records = {record["offset"]: record for record in description["records"]}
    # Not every case lists all of the records, and we do not read all of them
    # (the free segments, the blobs of an RNTuple, some baskets)
    found = {offset: key for offset, key in trace.keys.items() if offset in records}
    assert found, "None of the records was read"
    for offset, key in found.items():
        record = records[offset]
        assert key.header.fNbytes == record["nbytes"]
        assert key.fClassName.fString.decode() == record["class"]
        if "name" in record:
            assert key.fName.fString.decode() == record["name"]
            assert key.header.fCycle == record["cycle"]
            assert key.fSeekPdir == record["seek_pdir"]
