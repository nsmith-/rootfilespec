import dataclasses
from collections.abc import Hashable
from typing import Any, Generic, TypeVar

import numpy as np

from rootfilespec.bootstrap.streamedobject import StreamHeader
from rootfilespec.buffer import ReadBuffer
from rootfilespec.serializable import (
    AssociativeContainerSerDe,
    ContainerSerDe,
    Members,
    MemberSerDe,
    MemberType,
    ReadObjMethod,
    _ObjectReader,
)


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
            if pad == b"\x00":
                # This is the null pad byte that indicates an empty array (even if n > 0)
                members[self.name] = np.array([], dtype=self.dtype)
                return members, buffer
            if pad != b"\x01":
                msg = f"Expected null or 0x01 pad byte but got {pad!r} for size {n}"
                raise ValueError(msg)
            if n == 0:
                msg = "Array size is 0 but pad byte is not null"
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
        data, buffer = buffer.consume(self.size * self.dtype.itemsize)
        arg = np.frombuffer(data, dtype=self.dtype, count=self.size)
        members[self.name] = arg
        return members, buffer


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


T = TypeVar("T", bound=MemberType)


@dataclasses.dataclass
class _StdVectorReader:
    name: str
    inner_reader: ReadObjMethod
    hasheader: bool = True
    """When vectors are nested, the StreamHeader is not present in the inner vector."""

    def __call__(
        self, members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        members[self.name], buffer = StdVector.read_as(
            self.inner_reader, self.hasheader, buffer
        )
        return members, buffer


@dataclasses.dataclass
class StdVector(ContainerSerDe, Generic[T]):
    """A class to represent a std::vector<T>."""

    items: list[T]
    """The items in the vector."""

    @classmethod
    def build_reader(cls, fname: str, inner_reader: ReadObjMethod):
        """Build a reader for the std::vector<T>."""
        if isinstance(inner_reader, _ObjectReader) and isinstance(
            inner_reader.membermethod, _StdVectorReader
        ):
            inner_reader.membermethod.hasheader = False
        return _StdVectorReader(fname, inner_reader)

    @classmethod
    def read_as(cls, inner_reader: ReadObjMethod, hasheader: bool, buffer: ReadBuffer):
        if hasheader:
            header, buffer = StreamHeader.read(buffer)
            # TODO: byte count check
            if header.is_memberwise():
                msg = "Memberwise reading of StdVector not implemented"
                raise NotImplementedError(msg)
        (n,), buffer = buffer.unpack(">i")
        items: list[T] = []
        for _ in range(n):
            obj, buffer = inner_reader(buffer)
            items.append(obj)
        return cls(items), buffer


@dataclasses.dataclass
class StdSet(ContainerSerDe, Generic[T]):
    """A class to represent a std::set<T>."""

    items: set[T]
    """The items in the set."""

    @classmethod
    def build_reader(cls, fname: str, inner_reader: ReadObjMethod):  # noqa: ARG003
        def update_members(members: Members, buffer: ReadBuffer):
            msg = "StdSet not implemented"
            raise NotImplementedError(msg)
            # (n,), buffer = buffer.unpack(">i")
            # items: set[T] = set()
            # for _ in range(n):
            #     obj, buffer = inner_reader(buffer)
            #     items.add(obj)
            # members[fname] = items
            # return members, buffer

        return update_members


@dataclasses.dataclass
class StdDeque(ContainerSerDe, Generic[T]):
    """A class to represent a std::set<T>."""

    items: set[T]
    """The items in the set."""

    @classmethod
    def build_reader(cls, fname: str, inner_reader: ReadObjMethod):  # noqa: ARG003
        def update_members(members: Members, buffer: ReadBuffer):
            msg = "StdDeque not implemented"
            raise NotImplementedError(msg)

        return update_members


K = TypeVar("K", bound=Hashable)
V = TypeVar("V", bound=MemberType)


@dataclasses.dataclass
class StdMap(AssociativeContainerSerDe, Generic[K, V]):
    """A class to represent a std::map<K, V>."""

    items: dict[K, V]
    """The items in the map."""

    @classmethod
    def build_reader(
        cls, fname: str, key_reader: ReadObjMethod, value_reader: ReadObjMethod
    ):
        def update_members(members: Members, buffer: ReadBuffer):
            members[fname], buffer = cls.read_as(key_reader, value_reader, buffer)
            return members, buffer

        return update_members

    @classmethod
    def read_as(
        cls, key_reader: ReadObjMethod, value_reader: ReadObjMethod, buffer: ReadBuffer
    ):
        header, buffer = StreamHeader.read(buffer)
        if header.is_memberwise():
            msg = "Suspicious map with memberwise reading, incorrect length seen in uproot-issue465-flat.root"
            raise NotImplementedError(msg)
        (n,), buffer = buffer.unpack(">i")
        items: dict[K, V] = {}
        for _ in range(n):
            key, buffer = key_reader(buffer)
            value, buffer = value_reader(buffer)
            items[key] = value
        return cls(items), buffer


@dataclasses.dataclass
class StdPair(AssociativeContainerSerDe, Generic[K, V]):
    """A class to represent a std::pair<K, V>."""

    items: tuple[K, V]
    """The items in the pair."""

    @classmethod
    def build_reader(
        cls, fname: str, key_reader: ReadObjMethod, value_reader: ReadObjMethod
    ):
        def update_members(members: Members, buffer: ReadBuffer):
            members[fname], buffer = cls.read_as(key_reader, value_reader, buffer)
            return members, buffer

        return update_members

    @classmethod
    def read_as(
        cls, key_reader: ReadObjMethod, value_reader: ReadObjMethod, buffer: ReadBuffer
    ):
        raise NotImplementedError
        # key, buffer = key_reader(buffer)
        # value, buffer = value_reader(buffer)
        # return cls((key, value)), buffer
