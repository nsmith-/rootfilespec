from __future__ import annotations

import dataclasses
import struct
from functools import partial
from inspect import get_annotations  # type: ignore[attr-defined]
from typing import (
    Annotated,
    Any,
    Callable,
    Generic,
    TypeVar,
    get_args,
    get_origin,
    get_type_hints,
)

import numpy as np
from typing_extensions import dataclass_transform  # in typing for Python 3.11+


@dataclasses.dataclass
class ReadBuffer:
    """A ReadBuffer is a memoryview that keeps track of the absolute and relative
    positions of the data it contains.

    Attributes:
        data (memoryview): The data contained in the buffer.
        abspos (int | None): The absolute position of the buffer in the file.
            If the buffer was created from a compressed buffer, this will be None.
        relpos (int): The relative position of the buffer from the start of the TKey.
        local_refs (dict[int, bytes], optional): A dictionary of local references that may
            be found in the buffer (for use reading StreamHeader data)
    """

    data: memoryview
    abspos: int | None
    relpos: int
    local_refs: dict[int, bytes] = dataclasses.field(default_factory=dict)

    def __getitem__(self, key: slice) -> ReadBuffer:
        """Get a slice of the buffer."""
        start: int = key.start or 0
        if start > len(self.data):
            msg = f"Cannot get slice {key} from buffer of length {len(self.data)}"
            raise IndexError(msg)
        return ReadBuffer(
            self.data[key],
            self.abspos + start if self.abspos is not None else None,
            self.relpos + start,
            self.local_refs,
        )

    def __len__(self) -> int:
        """Get the length of the buffer."""
        return len(self.data)

    def __repr__(self) -> str:
        """Get a string representation of the buffer."""
        return (
            f"ReadBuffer size {len(self.data)} at abspos={self.abspos}, relpos={self.relpos}"
            "\n  local_refs: "
            + "".join(f"\n    {k}: {v!r}" for k, v in self.local_refs.items())
            + "\n  data[:0x100]: "
            + "".join(
                f"\n    0x{i:03x} | "
                + self.data[i : i + 16].hex(sep=" ")
                + " | "
                + "".join(
                    chr(c) if 32 <= c < 127 else "." for c in self.data[i : i + 16]
                )
                for i in range(0, min(256, len(self)), 16)
            )
        )

    def __bool__(self) -> bool:
        return bool(self.data)

    def unpack(self, fmt: str | struct.Struct) -> tuple[tuple[Any, ...], ReadBuffer]:
        """Unpack the buffer according to the given format."""
        if isinstance(fmt, struct.Struct):
            return fmt.unpack(self.data[: fmt.size]), self[fmt.size :]
        size = struct.calcsize(fmt)
        return struct.unpack(fmt, self.data[:size]), self[size:]

    def consume(self, size: int) -> tuple[bytes, ReadBuffer]:
        """Consume the given number of bytes from the buffer."""
        if size > len(self.data):
            msg = f"Cannot consume {size} bytes from buffer of length {len(self.data)}"
            raise IndexError(msg)
        if size < 0:
            msg = (
                f"Cannot consume a negative number of bytes: {size=}, {self.__len__()=}"
            )
            raise ValueError(msg)
        return bytes(self.data[:size]), self[size:]

    def info(self) -> str:
        return f"buffer size={self.__len__()} at abspos={self.abspos}, relpos={self.relpos}"


DataFetcher = Callable[[int, int], ReadBuffer]
Args = tuple[Any, ...]
ReadMethod = Callable[[ReadBuffer, Args], tuple[Args, ReadBuffer]]
OutType = TypeVar("OutType")


def _read_wrapper(cls: type[ROOTSerializable]) -> ReadMethod:
    """A wrapper to call the read method of a ROOTSerializable class."""

    def read(buffer: ReadBuffer, args: Args) -> tuple[Args, ReadBuffer]:
        obj, buffer = cls.read(buffer)
        return (*args, obj), buffer

    return read


@dataclasses.dataclass
class Fmt:
    """A class to hold the format of a field."""

    fmt: str

    def read_as(
        self, outtype: type[OutType], buffer: ReadBuffer, args: Args
    ) -> tuple[Args, ReadBuffer]:
        tup, buffer = buffer.unpack(self.fmt)
        return (*args, outtype(*tup)), buffer


@dataclasses.dataclass
class BasicArray:
    """A class to hold a basic array of a given type.

    Attributes:
        dtype (np.dtype): The format of the array.
        shapefield (str): The field in the parent object holding the
            shape of the array.
    """

    dtype: np.dtype[Any]
    shapefield: str


@dataclasses.dataclass
class _BasicArrayReadMethod:
    dtype: np.dtype[Any]
    sizeidx: int

    def read(self, buffer: ReadBuffer, args: Args) -> tuple[Args, ReadBuffer]:
        n = args[self.sizeidx]
        pad, buffer = buffer.consume(1)
        if not ((n == 0 and pad == b"\x00") or (n > 0 and pad == b"\x01")):
            msg = f"Expected null or 0x01 pad byte but got {pad!r}"
            raise ValueError(msg)
        data, buffer = buffer.consume(n * self.dtype.itemsize)
        arg = np.frombuffer(data, dtype=self.dtype, count=n)
        return (*args, arg), buffer


T = TypeVar("T", bound="ROOTSerializable")


@dataclasses.dataclass
class ROOTSerializable:
    """
    A base class for objects that can be serialized and deserialized from a buffer.
    """

    @classmethod
    def read(cls: type[T], buffer: ReadBuffer) -> tuple[T, ReadBuffer]:
        members, buffer = cls.read_members(buffer)
        return cls(*members), buffer

    @classmethod
    def read_members(cls, buffer: ReadBuffer) -> tuple[tuple[Any, ...], ReadBuffer]:
        msg = "Unimplemented method: {cls.__name__}.read_members"
        raise NotImplementedError(msg)


@dataclasses.dataclass
class Pointer(ROOTSerializable, Generic[T]):
    obj: int | None

    @classmethod
    def read(cls, buffer: ReadBuffer):
        (addr,), buffer = buffer.unpack(">i")
        if not addr:
            return cls(None), buffer
        # TODO: register all objects in something like local_refs so we can dereference them here
        # obj, buffer = outtype.read(buffer)
        return cls(addr), buffer


@dataclasses.dataclass
class StdVector(ROOTSerializable, Generic[T]):
    """A class to represent a std::vector<T>.

    Attributes:
        items (list[T]): The list of objects in the vector.
    """

    items: list[T]

    @classmethod
    def read_as(
        cls, outtype: type[T], buffer: ReadBuffer, args: Args
    ) -> tuple[Args, ReadBuffer]:
        raise NotImplementedError
        # (n,), buffer = buffer.unpack(">i")
        # out: list[T] = []
        # for _ in range(n):
        #     obj, buffer = outtype.read(buffer)
        #     out.append(obj)
        # return (*args, cls(out)), buffer


@dataclass_transform()
def serializable(cls: type[T]) -> type[T]:
    """A decorator to add a read_members method to a class that reads its fields from a buffer.

    The class must have type hints for its fields, and the fields must be of types that
    either have a read method or are subscripted with a Fmt object.
    """
    cls = dataclasses.dataclass(cls)

    # if the class already has a read_members method, don't overwrite it
    readmethod = getattr(cls, "read_members", None)
    if (
        readmethod
        and getattr(readmethod, "__qualname__", None)
        == f"{cls.__qualname__}.read_members"
    ):
        return cls

    names: list[str] = []
    constructors: list[ReadMethod] = []
    namespace = get_type_hints(cls, include_extras=True)
    for field in get_annotations(cls):
        names.append(field)
        ftype = namespace[field]
        if isinstance(ftype, type) and issubclass(ftype, ROOTSerializable):
            constructors.append(_read_wrapper(ftype))
        elif origin := get_origin(ftype):
            if origin is Annotated:
                ftype, *annotations = get_args(ftype)
                if not annotations:
                    msg = f"Cannot read field {field} of type {ftype} (missing annotations)"
                    raise NotImplementedError(msg)
                format, *annotations = annotations
                if isinstance(format, Fmt):
                    # TODO: potential optimization: consecutive struct fields could be read in one struct.unpack call
                    constructors.append(partial(format.read_as, ftype))
                elif isinstance(format, BasicArray):
                    assert ftype is np.ndarray
                    fieldindex = names.index(format.shapefield)
                    constructors.append(
                        _BasicArrayReadMethod(format.dtype, fieldindex).read
                    )
                else:
                    msg = f"Cannot read field {field} of type {ftype} with format {format}"
                    raise NotImplementedError(msg)
            elif origin is Pointer:
                constructors.append(_read_wrapper(origin))
            elif origin is StdVector:
                (ftype,) = get_args(ftype)
                constructors.append(partial(StdVector.read_as, ftype))
            else:
                msg = f"Cannot read field {field} of subscripted type {ftype} with origin {origin}"
                raise NotImplementedError(msg)
        else:
            msg = f"Cannot read field {field} of type {ftype}"
            raise NotImplementedError(msg)

    @classmethod  # type: ignore[misc]
    def read_members(_: type[T], buffer: ReadBuffer) -> tuple[Args, ReadBuffer]:
        args: Args = ()
        for constructor in constructors:
            args, buffer = constructor(buffer, args)
        return args, buffer

    cls.read_members = read_members  # type: ignore[assignment]
    return cls
