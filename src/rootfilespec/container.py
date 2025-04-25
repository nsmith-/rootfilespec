import dataclasses
from typing import Any, Generic, Optional, TypeVar, get_args, get_origin

import numpy as np

from rootfilespec.structutil import (
    ContainerSerDe,
    Members,
    MemberSerDe,
    ReadBuffer,
    ReadMembersMethod,
    ReadObjMethod,
    ROOTSerializable,
)

T = TypeVar("T", bound="ROOTSerializable")


@dataclasses.dataclass
class _ArrayReader:
    """Arrays whose length is set by another member and have a pad byte between them"""

    name: str
    dtype: np.dtype[Any]
    sizevar: str
    haspad: bool
    """Whether the array has a pad byte or not"""

    def __call__(
        self, members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        n = members[self.sizevar]
        if self.haspad:
            pad, buffer = buffer.consume(1)
            if not ((n == 0 and pad == b"\x00") or (n > 0 and pad == b"\x01")):
                msg = f"Expected null or 0x01 pad byte but got {pad!r} for size {n}"
                raise ValueError(msg)
        data, buffer = buffer.consume(n * self.dtype.itemsize)
        members[self.name] = np.frombuffer(data, dtype=self.dtype, count=n)
        return members, buffer


@dataclasses.dataclass
class BasicArray(MemberSerDe):
    """A class to hold a basic array of a given type."""

    dtype: str
    shapefield: str
    """The field that holds the shape of the array."""
    haspad: bool = True
    """Whether the array has a pad byte or not"""

    def build_reader(self, fname: str, ftype: type):  # noqa: ARG002
        return _ArrayReader(fname, np.dtype(self.dtype), self.shapefield, self.haspad)


@dataclasses.dataclass
class _CArrayReader:
    """Array that has its length at the beginning of the array and has no pad byte"""

    name: str
    dtype: np.dtype[Any]

    def __call__(
        self, members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        (n,), buffer = buffer.unpack(">i")
        data, buffer = buffer.consume(n * self.dtype.itemsize)
        members[self.name] = np.frombuffer(data, dtype=self.dtype, count=n)
        return members, buffer


@dataclasses.dataclass
class CArray(MemberSerDe):
    """A class to hold a C array of a given type."""

    dtype: str

    def build_reader(self, fname: str, ftype: type):  # noqa: ARG002
        return _CArrayReader(fname, np.dtype(self.dtype))


@dataclasses.dataclass
class FixedSizeArray(MemberSerDe):
    """A class to hold a fixed size array of a given type.

    Attributes:
        dtype (np.dtype): The format of the array.
        size (int): The size of the array.
    """

    dtype: np.dtype[Any]
    size: int

    def build_reader(self, fname: str, ftype: type):  # noqa: ARG002
        def read(members: Members, buffer: ReadBuffer) -> tuple[Members, ReadBuffer]:
            data, buffer = buffer.consume(self.size * self.dtype.itemsize)
            arg = np.frombuffer(data, dtype=self.dtype, count=self.size)
            members[fname] = arg
            return members, buffer

        return read


class Ref(Generic[T]):
    """A class to hold a reference to an object.

    We cannot use a dataclass here because its repr might end up
    being cyclic and cause a stack overflow.
    """

    obj: Optional[T]
    """The object that is referenced."""

    def __init__(self, obj: Optional[T]):
        self.obj = obj

    def __repr__(self):
        label = type(self.obj).__name__ if self.obj else "None"
        return f"Ref({label})"

    @classmethod
    def read_as(cls, ftype: type[T], buffer: ReadBuffer):  # noqa: ARG003
        (addr,), _ = buffer.unpack(">i")
        if not addr:
            buffer = buffer[4:]
            return cls(None), buffer
        if addr & 0x40000000:
            # this isn't actually an address but an object
            addr &= ~0x40000000
            buffer = buffer[addr + 4 :]
            return cls(None), buffer
            # obj, buffer = ftype.read(buffer)
            # return cls(obj), buffer
        # TODO: finish Pointer implementation
        return cls(None), buffer[4:]
        # msg = f"Pointer to address {addr} not implemented"
        # raise NotImplementedError(msg)


@dataclasses.dataclass
class Pointer(MemberSerDe):
    def build_reader(self, fname: str, ftype: type):
        if (origin := get_origin(ftype)) is not Ref:
            msg = f"Pointer() only can be used with Ref, got {origin}"
            raise ValueError(msg)
        (ftype,) = get_args(ftype)
        if not issubclass(ftype, ROOTSerializable):
            msg = f"Pointer() only can be used with Ref[ROOTSerializable], got {ftype}"
            raise ValueError(msg)

        def read(members: Members, buffer: ReadBuffer) -> tuple[Members, ReadBuffer]:
            obj, buffer = Ref.read_as(ftype, buffer)
            members[fname] = obj
            return members, buffer

        return read


@dataclasses.dataclass
class StdVector(ROOTSerializable, ContainerSerDe[T]):
    """A class to represent a std::vector<T>."""

    items: tuple[T, ...]
    """The items in the vector."""

    @classmethod
    def build_reader(
        cls, fname: str, inner_reader: ReadObjMethod[T]
    ) -> ReadMembersMethod:
        def update_members(members: Members, buffer: ReadBuffer):
            (n,), buffer = buffer.unpack(">i")
            items: tuple[T, ...] = ()
            for _ in range(n):
                obj, buffer = inner_reader(buffer)
                items += (obj,)
            members[fname] = items
            return members, buffer

        return update_members
