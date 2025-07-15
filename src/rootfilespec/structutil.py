import dataclasses
from typing import get_args

from rootfilespec.buffer import ReadBuffer
from rootfilespec.serializable import Members, MemberSerDe


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
class _OptionalFieldReader:
    """A class to read an optional field from a buffer."""

    fname: str
    fmt: str
    flagname: str
    flagvalue: int
    ftype: type

    def __call__(
        self, members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        if members[self.flagname] & self.flagvalue:
            tup, buffer = buffer.unpack(self.fmt)
            members[self.fname] = self.ftype(*tup)
        else:
            members[self.fname] = None
        return members, buffer


@dataclasses.dataclass
class OptionalField(MemberSerDe):
    """A class to hold an optional field format.
    Optional fields are fields that may or may not be present in the data.
    They are only read if the value of the flag from flagname matches flagvalue."""

    fmt: str
    flagname: str
    flagvalue: int

    def build_reader(self, fname: str, ftype: type):
        ftype, _ = get_args(ftype)  # Get the type inside Optional
        return _OptionalFieldReader(
            fname, self.fmt, self.flagname, self.flagvalue, ftype
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
