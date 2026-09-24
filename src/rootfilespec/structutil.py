import dataclasses
import operator
from typing import get_args

from rootfilespec.serializable import Members, MemberSerDe, ReadBuffer, ROOTSerializable


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


@dataclasses.dataclass
class _CountedStringReader:
    fname: str
    length_fmt: str

    def __call__(
        self, members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        (length,), buffer = buffer.unpack(self.length_fmt)
        members[self.fname], buffer = buffer.consume(length)
        return members, buffer


@dataclasses.dataclass
class CountedString(MemberSerDe):
    """A string stored as its length, then that many bytes, read as plain ``bytes``

    ``length_fmt`` is the struct format of the length, e.g. ``"<I"`` for an
    RNTuple string (a 32-bit little-endian unsigned length). The bytes are not
    decoded: a reader should not assume an encoding the format does not promise.
    """

    length_fmt: str

    def build_reader(self, fname: str, ftype: type):  # noqa: ARG002
        return _CountedStringReader(fname, self.length_fmt)


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
