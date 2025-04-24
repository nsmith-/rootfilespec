import dataclasses
import sys
from functools import partial
from typing import (
    Annotated,
    Any,
    Callable,
    TypeVar,
    get_args,
    get_origin,
    get_type_hints,
)

import numpy as np
from typing_extensions import dataclass_transform

from rootfilespec.container import Pointer, StdVector
from rootfilespec.structutil import (
    Args,
    MemberSerDe,
    ReadBuffer,
    ROOTSerializable,
)

T = TypeVar("T", bound="ROOTSerializable")
ReadMethod = Callable[[ReadBuffer, Args], tuple[Args, ReadBuffer]]


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


def _read_wrapper(cls: type["ROOTSerializable"]) -> ReadMethod:
    """A wrapper to call the read method of a ROOTSerializable class."""

    def read(buffer: ReadBuffer, args: Args) -> tuple[Args, ReadBuffer]:
        obj, buffer = cls.read(buffer)
        return (*args, obj), buffer

    return read


def _get_read_method(ftype) -> ReadMethod:
    if isinstance(ftype, type) and issubclass(ftype, ROOTSerializable):
        return _read_wrapper(ftype)
    if origin := get_origin(ftype):
        if origin is Annotated:
            ftype, *annotations = get_args(ftype)
            memberserde = next(
                (ann for ann in annotations if isinstance(ann, MemberSerDe)), None
            )
            if memberserde:
                return memberserde.build_reader(ftype)
            msg = f"Cannot read type {ftype} with annotations {annotations}"
            raise NotImplementedError(msg)
        if origin is Pointer:
            return _read_wrapper(origin)
        if origin is StdVector:
            (ftype,) = get_args(ftype)
            # TODO: nested std::vectors here instead of in StdVector.read_as
            return partial(StdVector.read_as, ftype)
        msg = f"Cannot read subscripted type {ftype} with origin {origin}"
        raise NotImplementedError(msg)
    msg = f"Cannot read type {ftype}"
    raise NotImplementedError(msg)


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
        constructors.append(_get_read_method(ftype))

    @classmethod  # type: ignore[misc]
    def read_members(_: type[T], buffer: ReadBuffer) -> tuple[Args, ReadBuffer]:
        args: Args = ()
        for constructor in constructors:
            args, buffer = constructor(buffer, args)
        return args, buffer

    cls.read_members = read_members  # type: ignore[assignment]
    return cls
