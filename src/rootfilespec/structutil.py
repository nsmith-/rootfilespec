import dataclasses
import operator
from typing import Literal, get_args

from rootfilespec.serializable import (
    Members,
    MemberSerDe,
    ReadBuffer,
    ROOTSerializable,
    read_value,
)


@dataclasses.dataclass
class _FmtReader:
    fname: str
    fmt: str
    outtype: type

    def __call__(
        self, members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        if self.fmt in ("float16", "charstar"):
            msg = f"Unimplemented format {self.fmt}"
            raise NotImplementedError(msg)
        tup, buffer = buffer.unpack(self.fmt)
        members[self.fname] = self.outtype(*tup)
        return members, buffer


@dataclasses.dataclass
class Fmt(MemberSerDe):
    """A class to hold the format of a field."""

    fmt: str

    def build_reader(self, fname: str, ftype: type):
        return _FmtReader(fname, self.fmt, ftype)


StringEncoding = Literal["RNTuple", "TString", "charstar"]
"""The on-disk string encodings (root-io-spec Conventions §5)

- ``"RNTuple"``: a u32 little-endian length, then that many bytes.
- ``"TString"``: the counted string, one length byte, or 255 then a u32
  big-endian length (§5.1). ``TString`` and ``std::string`` use it.
- ``"charstar"``: an i32 big-endian length, then that many bytes, with no
  terminator and no 255 escape: a ``char*`` member (``kCharStar``, §5.4) and a
  ``TStringLong`` (§5.1.1). A length of zero or less means no bytes follow, so a
  null pointer reads as ``b""``.
"""


@dataclasses.dataclass
class _ROOTStringReader:
    fname: str
    serde: "ROOTString"

    def __call__(
        self, members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        members[self.fname], buffer = self.serde.read(buffer)
        return members, buffer


@dataclasses.dataclass
class ROOTString(MemberSerDe):
    """A string, read as plain ``bytes``: ``Annotated[bytes, ROOTString(...)]``

    The bytes are not decoded: ROOT strings are uninterpreted bytes, and a reader
    should not assume an encoding (root-io-spec Conventions §5.1). Nor do they
    say which encoding they came from: the annotation, or a key's class name,
    holds that. A string read through a pointer loses it (``docs/design.md``).
    """

    encoding: StringEncoding
    """How the length and the bytes are stored, one of :data:`StringEncoding`"""
    framed: bool = False
    """Whether a byte count and version word come first, as for a ``std::string``
    data member (§5.3). Read with :class:`StreamHeader`, as other framed members are."""

    def __post_init__(self) -> None:
        if self.encoding not in get_args(StringEncoding):
            msg = f"Unknown string encoding {self.encoding!r}"
            raise ValueError(msg)

    def read(self, buffer: ReadBuffer) -> tuple[bytes, ReadBuffer]:
        """Read one string from the buffer"""
        if self.framed:
            return self._read_framed(buffer)
        if self.encoding == "RNTuple":
            (length,), buffer = buffer.unpack("<I")
        elif self.encoding == "TString":
            (length,), buffer = buffer.unpack(">B")
            if length == 255:
                (length,), buffer = buffer.unpack(">I")
        else:  # "charstar", checked in __post_init__
            (length,), buffer = buffer.unpack(">i")
            length = max(length, 0)
        return buffer.consume(length)

    def _read_framed(self, buffer: ReadBuffer) -> tuple[bytes, ReadBuffer]:
        # Imported here: rootfilespec.bootstrap imports this module (#119)
        from rootfilespec.bootstrap.streamedobject import StreamHeader

        header, buffer = StreamHeader.read(buffer)
        if header.fVersion is None:
            msg = f"Expected a byte count and version before the string, got {header}"
            raise ValueError(msg)
        start = buffer.relpos
        data, buffer = ROOTString(self.encoding).read(buffer)
        if buffer.relpos - start != header.fByteCount - 2:
            msg = f"String byte count {header.fByteCount} does not match its {len(data)}-byte string"
            raise ValueError(msg)
        return data, buffer

    def build_reader(self, fname: str, ftype: type):  # noqa: ARG002
        return _ROOTStringReader(fname, self)


@dataclasses.dataclass
class _OptionalFieldReader:
    """A class to read an optional field from a buffer."""

    fname: str
    fmt: str
    flagname: str
    operation: str
    flagvalue: int
    ftype: type

    def __call__(
        self, members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        flag = members[self.flagname]
        ops = {
            "&": lambda a, b: a & b,
            "==": operator.eq,
            "!=": operator.ne,
            ">=": operator.ge,
            "<=": operator.le,
            ">": operator.gt,
            "<": operator.lt,
        }
        op_func = ops.get(self.operation)
        if op_func is None:
            msg = f"Unsupported operation: {self.operation}. Supported operations: {', '.join(ops.keys())}"
            raise ValueError(msg)
        if op_func(flag, self.flagvalue):
            if self.fmt == "class":
                if not issubclass(self.ftype, ROOTSerializable):
                    msg = f"Expected ftype to be a subclass of ROOTSerializable, got {self.ftype}"
                    raise TypeError(msg)
                members[self.fname], buffer = self.ftype.read(buffer)
            else:
                tup, buffer = buffer.unpack(self.fmt)
                members[self.fname] = self.ftype(*tup)
        else:
            members[self.fname] = None
        return members, buffer


@dataclasses.dataclass
class OptionalField(MemberSerDe):
    """A class to hold an optional field format.
    Optional fields are fields that may or may not be present in the data.
    They are only read if the value of the flag from flagname matches flagvalue
    according to the specified operation."""

    fmt: str
    flagname: str
    operation: str
    flagvalue: int

    def build_reader(self, fname: str, ftype: type):
        ftype, _ = get_args(ftype)  # Get the type inside Optional
        return _OptionalFieldReader(
            fname, self.fmt, self.flagname, self.operation, self.flagvalue, ftype
        )


@dataclasses.dataclass
class _IfBytesRemainReader:
    fname: str
    ftype: object

    def __call__(
        self, members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        if buffer:
            members[self.fname], buffer = read_value(self.ftype, buffer)
        else:
            members[self.fname] = None
        return members, buffer


@dataclasses.dataclass
class IfBytesRemain(MemberSerDe):
    """A last member that only later format versions write

    ``Annotated[T | None, IfBytesRemain()]`` is read as T if any bytes remain,
    and is None if none do. The buffer must end where the enclosing structure
    does, as an envelope's payload ends before its checksum.
    """

    def build_reader(self, fname: str, ftype: type):
        ftype, _ = get_args(ftype)  # The type inside Optional
        return _IfBytesRemainReader(fname, ftype)


@dataclasses.dataclass
class StdBitset(MemberSerDe):
    """A class to hold a std::bitset of a given size."""

    size: int

    def build_reader(self, fname: str, ftype: type):  # noqa: ARG002
        # fmt = ">I"
        # if self.size > 32:
        #     fmt = ">Q"
        # if self.size > 64:
        #     msg = f"Unimplemented size {self.size}"
        #     raise NotImplementedError(msg)

        # return _FmtReader(fname, fmt, ftype)
        def reader(members: Members, buffer: ReadBuffer) -> tuple[Members, ReadBuffer]:
            msg = "StdBitset reader not implemented"
            raise NotImplementedError(msg)

        return reader
