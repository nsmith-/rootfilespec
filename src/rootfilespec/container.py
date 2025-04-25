import dataclasses
from typing import Any, TypeVar

import numpy as np

from rootfilespec.buffer import ReadBuffer
from rootfilespec.serializable import (
    ContainerSerDe,
    Members,
    MemberSerDe,
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

    fmt: str
    shapefield: str
    """The field that holds the shape of the array."""
    haspad: bool = True
    """Whether the array has a pad byte or not"""

    def build_reader(self, fname: str, ftype: type):  # noqa: ARG002
        if self.fmt in ("float16", "double32", "charstar"):
            msg = f"Unimplemented format {self.fmt}"
            raise NotImplementedError(msg)
        return _ArrayReader(fname, np.dtype(self.fmt), self.shapefield, self.haspad)


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
class _FixedSizeArrayReader:
    """Array that has its length at the beginning of the array and has no pad byte"""

    name: str
    dtype: np.dtype[Any]
    size: int

    def __call__(
        self, members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        msg = "FixedSizeArrayReader not implemented"
        raise NotImplementedError(msg)
        # data, buffer = buffer.consume(self.size * self.dtype.itemsize)
        # arg = np.frombuffer(data, dtype=self.dtype, count=self.size)
        # members[self.name] = arg
        # return members, buffer


@dataclasses.dataclass
class FixedSizeArray(MemberSerDe):
    """A class to hold a fixed size array of a given type.

    Attributes:
        dtype (np.dtype): The format of the array.
        size (int): The size of the array.
    """

    fmt: str
    size: int

    def build_reader(self, fname: str, ftype: type):  # noqa: ARG002
        if self.fmt in ("float16", "double32", "charstar"):
            msg = f"Unimplemented format {self.fmt}"
            raise NotImplementedError(msg)
        return _FixedSizeArrayReader(fname, np.dtype(self.fmt), self.size)


@dataclasses.dataclass
class StdVector(ROOTSerializable, ContainerSerDe[T]):
    """A class to represent a std::vector<T>."""

    items: tuple[T, ...]
    """The items in the vector."""

    @classmethod
    def build_reader(cls, fname: str, inner_reader: ReadObjMethod[T]):
        def update_members(members: Members, buffer: ReadBuffer):
            (n,), buffer = buffer.unpack(">i")
            items: tuple[T, ...] = ()
            for _ in range(n):
                obj, buffer = inner_reader(buffer)
                items += (obj,)
            members[fname] = items
            return members, buffer

        return update_members
