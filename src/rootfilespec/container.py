import dataclasses
from collections.abc import Hashable
from typing import Any, Generic, TypeVar, Union, get_args, get_origin

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
    _build_read,
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


@dataclasses.dataclass
class _ObjectArrayReader:
    """Array that has its length at the beginning of the array and has no pad byte"""

    name: str
    size: Union[int, str]
    inner_reader: ReadObjMethod

    def __call__(
        self, members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        if isinstance(self.size, int):
            n = self.size
        else:
            n = members[self.size]
            header, buffer = StreamHeader.read(buffer)
            if header.memberwise:
                msg = "Memberwise reading of ObjectArray not implemented"
                raise NotImplementedError(msg)
        items: list[MemberType] = []
        for _ in range(n):
            obj, buffer = self.inner_reader(buffer)
            items.append(obj)
        members[self.name] = items
        return members, buffer


@dataclasses.dataclass
class ObjectArray(MemberSerDe):
    """A class to hold an array of objects of a given type."""

    size: Union[int, str]
    """Either a fixed size or the name of a member that holds the size of the array."""

    def build_reader(self, fname: str, ftype: type):
        assert get_origin(ftype) is list, "ObjectArray must be used with a list type"
        inner_type, *_ = get_args(ftype)
        inner_reader = _build_read(inner_type)
        if isinstance(inner_reader, _ObjectReader) and isinstance(
            inner_reader.membermethod, _StdVectorReader
        ):
            # TODO: likely all nested StdVector have no header
            inner_reader.membermethod.hasheader = False
        return _ObjectArrayReader(fname, self.size, inner_reader)


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
            if header.memberwise:
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
        if header.memberwise:
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
