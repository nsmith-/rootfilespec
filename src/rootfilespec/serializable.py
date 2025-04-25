import dataclasses
import sys
from typing import (
    Annotated,
    Any,
    TypeVar,
    get_args,
    get_origin,
    get_type_hints,
)

from typing_extensions import dataclass_transform

from rootfilespec.structutil import (
    ContainerSerDe,
    Members,
    MemberSerDe,
    ReadBuffer,
    ReadMembersMethod,
    ROOTSerializable,
)

T = TypeVar("T", bound="ROOTSerializable")


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
class _ReadWrapper:
    fname: str
    objtype: type[ROOTSerializable]

    def __call__(self, members: Members, buffer: ReadBuffer):
        obj, buffer = self.objtype.read(buffer)
        members[self.fname] = obj
        return members, buffer


def _get_read_method(fname: str, ftype: Any) -> ReadMembersMethod:
    if isinstance(ftype, type) and issubclass(ftype, ROOTSerializable):
        return _ReadWrapper(fname, ftype)
    if origin := get_origin(ftype):
        if origin is Annotated:
            ftype, *annotations = get_args(ftype)
            memberserde = next(
                (ann for ann in annotations if isinstance(ann, MemberSerDe)), None
            )
            if memberserde:
                return memberserde.build_reader(fname, ftype)
            msg = f"Cannot read type {ftype} with annotations {annotations}"
            raise NotImplementedError(msg)
        if issubclass(origin, ContainerSerDe):
            ftype, *args = get_args(ftype)
            assert not args  # TODO: will not work for std::pair
            membermethod = _get_read_method("", ftype)

            def inner_reader(buffer: ReadBuffer):
                members, buffer = membermethod({}, buffer)
                return ftype(**members), buffer

            return origin.build_reader(fname, inner_reader)  # type: ignore[no-any-return]
        msg = f"Cannot read subscripted type {ftype} with origin {origin}"
        raise NotImplementedError(msg)
    msg = f"Cannot read type {ftype}"
    raise NotImplementedError(msg)


@dataclass_transform()
def serializable(cls: type[T]) -> type[T]:
    """A decorator to add a update_members method to a class that reads its fields from a buffer.

    The class must have type hints for its fields, and the fields must be of types that
    either have a read method or are subscripted with a Fmt object.
    """
    cls = dataclasses.dataclass(eq=False)(cls)

    # if the class already has a update_members method, don't overwrite it
    readmethod = getattr(cls, "update_members", None)
    if (
        readmethod
        and getattr(readmethod, "__qualname__", None)
        == f"{cls.__qualname__}.update_members"
    ):
        return cls

    # if the class has a self-reference, it will not be found in the default namespace
    localns = {cls.__name__: cls}
    namespace = get_type_hints(cls, localns=localns, include_extras=True)
    member_readers = [
        _get_read_method(field, namespace[field]) for field in _get_annotations(cls)
    ]

    # TODO: scan through and coalesce the _FmtReader objects into a single function call

    @classmethod  # type: ignore[misc]
    def update_members(
        _: type[T], members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        for reader in member_readers:
            members, buffer = reader(members, buffer)
        return members, buffer

    cls.update_members = update_members  # type: ignore[assignment]
    return cls
