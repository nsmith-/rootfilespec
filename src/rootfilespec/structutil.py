import dataclasses

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
        if self.fmt in ("float16", "double32", "charstar"):
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
