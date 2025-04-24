import dataclasses
from typing import Any, Generic, Optional, TypeVar, get_args

import numpy as np

from rootfilespec.structutil import Args, Fmt, MemberSerDe, ReadBuffer, ROOTSerializable

T = TypeVar("T", bound="ROOTSerializable")


@dataclasses.dataclass
class BasicArray(MemberSerDe):
    """A class to hold a basic array of a given type.

    Attributes:
        dtype (np.dtype): The format of the array.
        shapefield (str): The field in the parent object holding the
            shape of the array.
    """

    dtype: np.dtype[Any]
    shapefield: str

    def build_reader(self, ftype: type):  # noqa: ARG002
        def read(buffer: ReadBuffer, args: Args) -> tuple[Args, ReadBuffer]:
            msg = "Need to switch to Members for this"
            raise NotImplementedError(msg)

        return read


@dataclasses.dataclass
class FixedSizeArray:
    """A class to hold a fixed size array of a given type.

    Attributes:
        dtype (np.dtype): The format of the array.
        size (int): The size of the array.
    """

    dtype: np.dtype[Any]
    size: int

    def build_reader(self, ftype: type):  # noqa: ARG002
        def read(buffer: ReadBuffer, args: Args) -> tuple[Args, ReadBuffer]:
            data, buffer = buffer.consume(self.size * self.dtype.itemsize)
            arg = np.frombuffer(data, dtype=self.dtype, count=self.size)
            return (*args, arg), buffer

        return read

    def read(self, buffer: ReadBuffer, args: Args) -> tuple[Args, ReadBuffer]:
        data, buffer = buffer.consume(self.size * self.dtype.itemsize)
        arg = np.frombuffer(data, dtype=self.dtype, count=self.size)
        return (*args, arg), buffer


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
                    out, buffer = buffer.unpack(fmt.fmt)
                    out = ftype(out)
            else:
                msg = f"Cannot read field of type {outtype} with format {fmt}"
                raise NotImplementedError(msg)
        else:
            for _ in range(n):
                obj, buffer = outtype.read(buffer)
                out += (obj,)
        return (*args, cls(out)), buffer
