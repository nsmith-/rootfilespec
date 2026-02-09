import dataclasses
import struct
from collections.abc import Callable
from inspect import get_annotations
from typing import (
    Annotated,
    Any,
    TypeVar,
    get_args,
    get_origin,
    get_type_hints,
)

from typing_extensions import dataclass_transform

RT = TypeVar("RT", bound="ROOTSerializable")
MemberType = Any  # Union[int, float, bytes, str, bool, "ROOTSerializable"]
Members = dict[str, MemberType]


class FileContext:
    """Context for interpreting data from a given ROOT file

    This is abstract here because there are two stages of reading:
    1. In the initial stage, before the StreamerInfo is parsed, this
        context will be a "bootstrap" set of interpretations
    2. Once the StreamerInfo is parsed, a dynamically generated set
        of types will be used to read the rest of the file
    """

    def type_by_name(
        self, name: str, expect_version: int | None = None
    ) -> type["ROOTSerializable"]:
        """Lookup a ROOTSerializable-derived type by its name

        The name should be normalized according to dispatch.normalize()

        If expect_version is supplied, an exception is raised if the version
        does not match the version that is interpretable in this context
        """
        raise NotImplementedError()

    def type_by_checksum(self, checksum: bytes) -> type["ROOTSerializable"]:
        """Lookup ROOTSerializable-derived type by its StreamerInfo checksum"""
        raise NotImplementedError()


@dataclasses.dataclass
class BufferContext:
    """Holds local context for a buffer"""

    abspos: int | None
    """The absolute position of the buffer in the file.
        If the buffer was created from a compressed buffer, this will be None.
    """
    type_refs: dict[int, bytes] = dataclasses.field(
        default_factory=dict
    )  # TODO: type["ROOTSerializable"] (StreamHeader refactor)
    """Type references

    When a type first appears in the data, its name is used as a lookup key
    in the StreamerContext. Afterward, it is referenced by the relative position
    in the buffer where it first appeared
    """
    instance_refs: dict[int, "ROOTSerializable"] = dataclasses.field(
        default_factory=dict
    )
    """Instance references

    A Ref (pointer) may appear later in the buffer which may point to an arbitrary
    object, where the key is the position of the StreamHeader in the buffer, so all
    streamed object instances need to register themselves with that location once
    constructed
    """


@dataclasses.dataclass(repr=False)
class ReadBuffer:
    """A ReadBuffer is a memoryview that keeps track of the absolute and relative
    positions of the data it contains.

    """

    data: memoryview
    """The data contained in the buffer."""
    relpos: int
    """The relative position of the buffer from the start of the TKey"""
    file_context: FileContext
    """File-global context of the buffer"""
    context: BufferContext
    """Buffer-local context information"""

    def __getitem__(self, key: slice):
        """Get a slice of the buffer."""
        start: int = key.start or 0
        if start > len(self.data):
            msg = f"Cannot get slice {key} from buffer of length {len(self.data)}"
            raise IndexError(msg)
        return ReadBuffer(
            self.data[key],
            self.relpos + start,
            self.file_context,
            self.context,
        )

    def __len__(self) -> int:
        """Get the length of the buffer."""
        return len(self.data)

    def __repr__(self) -> str:
        """Get a string representation of the buffer."""
        return (
            f"ReadBuffer size {len(self.data)} at relpos={self.relpos}"
            f"\n  File context: {self.file_context}"
            f"\n  Local context: {self.context}"
            "\n  data[:0x100]: "
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

    def unpack(self, fmt: str) -> tuple[tuple[Any, ...], "ReadBuffer"]:
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


ReadMembersMethod = Callable[[Members, ReadBuffer], tuple[Members, ReadBuffer]]


def _get_annotations(cls: type) -> dict[str, Any]:
    """Get the annotations of a class, including private attributes.

    Only retrieves annotations from the class itself, not from its base classes.
    """
    return get_annotations(cls)


@dataclasses.dataclass
class ROOTSerializable:
    """
    A base class for objects that can be serialized and deserialized from a buffer.
    """

    @classmethod
    def read(cls, buffer: ReadBuffer):
        members: Members = {}
        # TODO: always loop through base classes? StreamedObject does this a special way
        members, buffer = cls.update_members(members, buffer)
        return cls(**members), buffer

    @classmethod
    def update_members(
        cls, members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        msg = f"Unimplemented method: {cls.__name__}.update_members"
        raise NotImplementedError(msg)


@dataclasses.dataclass
class _ReadWrapper:
    fname: str
    objtype: type[ROOTSerializable]

    def __call__(self, members: Members, buffer: ReadBuffer):
        obj, buffer = self.objtype.read(buffer)
        members[self.fname] = obj
        return members, buffer


@dataclasses.dataclass
class ReadObjMethod:
    """A wrapper to read a whole object from a buffer.

    Some containers will inspect the member method to determine how to read the
    object, for example, sometimes the StreamHeader will not be present
    """

    membermethod: ReadMembersMethod

    def __call__(self, buffer: ReadBuffer) -> tuple[MemberType, ReadBuffer]:
        members, buffer = self.membermethod({}, buffer)
        return members[""], buffer


class ContainerSerDe(ROOTSerializable):
    """A protocol for (De)serialization of generic container fields.

    The @serializable decorator will use these annotations to determine how to read
    the field from the buffer. For example, if a dataclass has a field of type
        `field: Container[Type]`
    Then `ContainerSerDe.build_reader(build_reader(Type))` will be called to get a function that
    can read the field from the buffer.
    """

    @classmethod
    def build_reader(cls, fname: str, inner_reader: ReadObjMethod) -> ReadMembersMethod:
        """Build a reader function for the given field name and inner read implementation.

        Implementation note:
        In principle, ReadObjMethod should be a generic type that
        accepts T, but since this is called at runtime the linter
        never sees the type, so the lower bound of MemberType is ok
        """
        msg = f"Cannot build reader for {cls.__name__}"
        raise NotImplementedError(msg)


class AssociativeContainerSerDe(ROOTSerializable):
    """A protocol for (De)serialization of generic associative container fields.

    The @serializable decorator will use these annotations to determine how to read
    the field from the buffer. For example, if a dataclass has a field of type
        `field: AssociativeContainer[KeyType, ValueType]`
    Then `AssociativeContainerSerDe.build_reader(build_reader(KeyType), build_reader(ValueType))`
    will be called to get a function that can read the field from the buffer.
    """

    @classmethod
    def build_reader(
        cls, fname: str, key_reader: ReadObjMethod, value_reader: ReadObjMethod
    ) -> ReadMembersMethod:
        """Build a reader function for the given field name and inner read implementation."""
        msg = f"Cannot build reader for {cls.__name__}"
        raise NotImplementedError(msg)


class MemberSerDe:
    """A protocol for Serialization/Deserialization method annotations for a field.

    The @serializable decorator will use these annotations to determine how to read
    the field from the buffer. For example, if a dataclass has a field of type
        `field: Annotated[Type, MemberSerDe(*args)]`
    Then `MemberSerDe.build_reader(Type)` will be called to get a function that
    can read the field from the buffer.
    """

    def build_reader(self, fname: str, ftype: type) -> ReadMembersMethod:
        """Build a reader function for the given field name and type.

        The reader function should take a ReadBuffer and return a tuple of the new
        arguments and the remaining buffer.
        """
        msg = f"Cannot build reader for {self.__class__.__name__}"
        raise NotImplementedError(msg)


def _build_read(ftype: type[MemberType]) -> ReadObjMethod:
    membermethod = _build_update_members("", ftype)
    return ReadObjMethod(membermethod)


def _build_update_members(fname: str, ftype: Any) -> ReadMembersMethod:
    if isinstance(ftype, type) and issubclass(ftype, ROOTSerializable):
        return _ReadWrapper(fname, ftype)
    if origin := get_origin(ftype):
        if origin is Annotated:
            itype, *annotations = get_args(ftype)
            memberserde = next(
                (ann for ann in annotations if isinstance(ann, MemberSerDe)), None
            )
            if memberserde:
                return memberserde.build_reader(fname, itype)
            msg = f"Cannot read type {itype} with annotations {annotations}"
            raise ValueError(msg)
        if isinstance(origin, type) and issubclass(origin, ContainerSerDe):
            itype, *args = get_args(ftype)
            assert not args
            item_reader = _build_read(itype)
            return origin.build_reader(fname, item_reader)
        if isinstance(origin, type) and issubclass(origin, AssociativeContainerSerDe):
            ktype, vtype, *args = get_args(ftype)
            assert not args
            key_reader = _build_read(ktype)
            value_reader = _build_read(vtype)
            return origin.build_reader(fname, key_reader, value_reader)
        msg = f"Cannot read subscripted type {ftype} with origin {origin}"
        raise ValueError(msg)
    msg = f"Cannot read type {ftype}"
    raise ValueError(msg)


@dataclass_transform()
def serializable(cls: type[RT]) -> type[RT]:
    """A decorator to add a update_members method to a class that reads its fields from a buffer.

    The class must have type hints for its fields, and the fields must be of types that
    either derive from ROOTSerializable or be annotated with a MemberSerDe instance
    """
    cls = dataclasses.dataclass(eq=True)(cls)

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
        _build_update_members(field, namespace[field])
        for field in _get_annotations(cls)
    ]

    # TODO: scan through and coalesce the _FmtReader objects into a single function call

    @classmethod  # type: ignore[misc]
    def update_members(
        _: type[RT], members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        for reader in member_readers:
            members, buffer = reader(members, buffer)
        return members, buffer

    cls.update_members = update_members  # type: ignore[assignment]
    return cls


DataFetcher = Callable[[int, int], ReadBuffer]
