import dataclasses
import struct
import sys
from functools import partial
from typing import (
    Annotated,
    Any,
    Callable,
    Generic,
    Optional,
    TypeVar,
    get_args,
    get_origin,
    get_type_hints,
)

import numpy as np
from typing_extensions import dataclass_transform  # in typing for Python 3.11+


def _get_annotations(cls: type) -> dict[str, Any]:
    """Get the annotations of a class, including private attributes."""
    if sys.version_info >= (3, 10):
        from inspect import get_annotations

        return get_annotations(cls)
    return {
        field: ann
        for field, ann in cls.__dict__.get("__annotations__", {}).items()
        if not field.startswith("_") and field != "self"
    }


Args = tuple[Any, ...]


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
    abspos: Optional[int]
    relpos: int
    local_refs: dict[int, bytes]

    def __init__(
        self,
        data: memoryview,
        abspos: Optional[int],
        relpos: int,
        local_refs: Optional[dict[int, bytes]] = None,
    ):
        self.data = data
        self.abspos = abspos
        self.relpos = relpos
        self.local_refs = local_refs or {}

    def __getitem__(self, key: slice):
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

    def unpack(self, fmt: str) -> tuple[Args, "ReadBuffer"]:
        """Unpack the buffer according to the given format."""
        size = struct.calcsize(fmt)
        out = struct.unpack(fmt, self.data[:size])
        return out, self[size:]

    def consume(self, size: int) -> tuple[bytes, "ReadBuffer"]:
        """Consume the given number of bytes from the buffer.

        Returns a copy of the data and the remaining buffer.
        """
        if size < 0:
            msg = (
                f"Cannot consume a negative number of bytes: {size=}, {self.__len__()=}"
            )
            raise ValueError(msg)
        out = self.data[:size].tobytes()
        return out, self[size:]

    def consume_view(self, size: int) -> tuple[memoryview, "ReadBuffer"]:
        """Consume the given number of bytes and return a view (not a copy).

        Use consume() to get a copy.
        """
        return self.data[:size], self[size:]


DataFetcher = Callable[[int, int], ReadBuffer]
Members = dict[str, Any]
ReadMethod = Callable[[ReadBuffer, Args], tuple[Args, ReadBuffer]]
OutType = TypeVar("OutType")


def _read_wrapper(cls: type["ROOTSerializable"]) -> ReadMethod:
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
            msg = f"Expected null or 0x01 pad byte but got {pad!r} for size {n}"
            raise ValueError(msg)
        data, buffer = buffer.consume(n * self.dtype.itemsize)
        arg = np.frombuffer(data, dtype=self.dtype, count=n)
        return (*args, arg), buffer


@dataclasses.dataclass
class FixedSizeArray:
    """A class to hold a fixed size array of a given type.

    Attributes:
        dtype (np.dtype): The format of the array.
        size (int): The size of the array.
    """

    dtype: np.dtype[Any]
    size: int

    def read(self, buffer: ReadBuffer, args: Args) -> tuple[Args, ReadBuffer]:
        data, buffer = buffer.consume(self.size * self.dtype.itemsize)
        arg = np.frombuffer(data, dtype=self.dtype, count=self.size)
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
    def read_members(cls, buffer: ReadBuffer) -> tuple[Args, ReadBuffer]:
        msg = "Unimplemented method: {cls.__name__}.read_members"
        raise NotImplementedError(msg)


@dataclasses.dataclass
class Pointer(ROOTSerializable, Generic[T]):
    obj: Optional[T]

    @classmethod
    def read(cls, buffer: ReadBuffer):
        (addr,), buffer = buffer.unpack(">i")
        if not addr:
            return cls(None), buffer
        # TODO: use read_streamed_item to read the object
        if addr & 0x40000000:
            # this isn't actually an address but an object
            addr &= ~0x40000000
            # skip forward
            buffer = buffer[addr:]
        return cls(None), buffer


@dataclasses.dataclass
class StdVector(ROOTSerializable, Generic[T]):
    """A class to represent a std::vector<T>.

    Attributes:
        items (list[T]): The list of objects in the vector.
    """

    items: tuple[T, ...]

    @classmethod
    def read_as(
        cls, outtype: type[T], buffer: ReadBuffer, args: Args
    ) -> tuple[Args, ReadBuffer]:
        (n,), buffer = buffer.unpack(">i")
        out: tuple[T, ...] = ()
        if outtype is StdVector:
            (interior_type,) = get_args(outtype)
            for _ in range(n):
                out, buffer = StdVector.read_as(interior_type, buffer, out)
        elif getattr(outtype, "_name", None) == "Annotated":
            # TODO: this should be handled in the serializable decorator
            (ftype, fmt) = get_args(outtype)
            if isinstance(fmt, Fmt):
                for _ in range(n):
                    out, buffer = fmt.read_as(ftype, buffer, out)
            else:
                msg = f"Cannot read field of type {outtype} with format {fmt}"
                raise NotImplementedError(msg)
        else:
            for _ in range(n):
                obj, buffer = outtype.read(buffer)
                out += (obj,)
        return (*args, cls(out)), buffer


@dataclass_transform()
def serializable(cls: type[T]) -> type[T]:
    """A decorator to add a read_members method to a class that reads its fields from a buffer.

    The class must have type hints for its fields, and the fields must be of types that
    either have a read method or are subscripted with a Fmt object.
    """
    cls = dataclasses.dataclass(eq=False)(cls)

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
    for field in _get_annotations(cls):
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
                    if format.shapefield not in names:
                        # TODO: to implement this we need to migrate read_members to return Members rather than Args
                        msg = f"Cannot yet read {field} because shape field {format.shapefield} is in base class"
                        raise NotImplementedError(msg)
                    fieldindex = names.index(format.shapefield)
                    constructors.append(
                        _BasicArrayReadMethod(format.dtype, fieldindex).read
                    )
                elif isinstance(format, FixedSizeArray):
                    assert ftype is np.ndarray
                    constructors.append(format.read)
                else:
                    msg = f"Cannot read field {field} of type {ftype} with format {format}"
                    raise NotImplementedError(msg)
            elif origin is Pointer:
                constructors.append(_read_wrapper(origin))
            elif origin is StdVector:
                (ftype,) = get_args(ftype)
                # TODO: nested std::vectors here instead of in StdVector.read_as
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
