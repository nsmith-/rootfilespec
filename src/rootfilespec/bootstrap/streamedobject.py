from dataclasses import dataclass
from enum import IntEnum
from typing import Generic, Optional, TypeVar, overload

from rootfilespec.buffer import ReadBuffer
from rootfilespec.dispatch import DICTIONARY, normalize
from rootfilespec.serializable import (
    ContainerSerDe,
    Members,
    MemberType,
    ReadObjMethod,
    ROOTSerializable,
    serializable,
)


class _StreamConstants(IntEnum):
    """Constants used for data members of StreamedObject class."""

    kByteCountMask = 0x40000000
    """Mask for ByteCount"""
    kClassMask = 0x80000000
    """Mask for ClassInfo"""
    kNewClassTag = 0xFFFFFFFF
    """New class tag"""
    kNotAVersion = 0x8000
    """Either kClassMask or kNewClassTag is set in bytes 4-5"""
    kStreamedMemberwise = 0x4000
    """Not sure if this is where it is used"""


@dataclass
class StreamHeader(ROOTSerializable):
    """Initial header for any streamed data object
    Reference: https://root.cern/doc/master/dobject.html
    Only one of fVersion, fClassName, or fClassRef will be set.
    """

    fByteCount: int
    """Number of remaining bytes in object (uncompressed)"""
    fVersion: Optional[int]
    """Version of Class"""
    fClassName: Optional[bytes]
    """Class name of object, if first instance of class in buffer"""
    fClassRef: Optional[int]
    """Position in buffer of class name if not specified here"""
    memberwise: bool
    """If the object is memberwise streamed"""

    @classmethod
    def read(cls, buffer: ReadBuffer):
        (fByteCount,), buffer = buffer.unpack(">i")
        if fByteCount & _StreamConstants.kByteCountMask:
            fByteCount &= ~_StreamConstants.kByteCountMask
        else:
            # This is a reference to another object in the buffer
            return cls(0, None, None, fByteCount, False), buffer
        fVersion, fClassName, fClassRef, memberwise = None, None, None, False
        (tmp1,), buffer = buffer.unpack(">H")
        if not (tmp1 & _StreamConstants.kNotAVersion):
            fVersion = tmp1
            if fVersion & _StreamConstants.kStreamedMemberwise:
                fVersion &= ~_StreamConstants.kStreamedMemberwise
                memberwise = True
                msg = "Memberwise streaming not implemented"
                raise NotImplementedError(msg)
            if fVersion == 0 and fByteCount >= 6:
                # This class is versioned by its streamer checksum instead
                (checksum,), buffer = buffer.unpack(">I")
                fVersion = checksum
            # TODO: understand fVersion == 0 when fByteCount == 2 (uproot-issue-222.root)
        else:
            fVersion = None
            (tmp2,), buffer = buffer.unpack(">H")
            fClassInfo: int = (tmp1 << 16) | tmp2
            if fClassInfo == _StreamConstants.kNewClassTag:
                fClassRef = buffer.relpos - 4
                fClassName = b""
                while True:
                    chr, buffer = buffer.consume(1)
                    if chr == b"\0":
                        break
                    fClassName += chr
                # Try to decode to ensure it is a valid name
                if not fClassName.decode("ascii").isprintable():
                    msg = f"Class name {fClassName!r} is not valid ASCII"
                    raise ValueError(msg)
                buffer.local_refs[fClassRef] = fClassName
            else:
                fClassRef = (fClassInfo & ~_StreamConstants.kClassMask) - 2
                fClassName = buffer.local_refs.get(fClassRef, None)
                if fClassName is None:
                    msg = f"ClassRef {fClassRef} not found in buffer local_refs (likely inside an uninterpreted object)"
                    raise NotImplementedError(msg)
        return cls(fByteCount, fVersion, fClassName, fClassRef, memberwise), buffer


@dataclass
class _RefReader:
    name: str
    inner_reader: ReadObjMethod

    def __call__(self, members: Members, buffer: ReadBuffer):
        members[self.name], buffer = read_streamed_item(buffer, self.inner_reader)
        return members, buffer


T = TypeVar("T", bound=MemberType)


class Ref(ContainerSerDe, Generic[T]):
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
    def build_reader(cls, fname: str, inner_reader: ReadObjMethod):
        return _RefReader(fname, inner_reader)


@overload
def read_streamed_item(
    buffer: ReadBuffer, method: ReadObjMethod
) -> tuple[Ref[MemberType], ReadBuffer]: ...


@overload
def read_streamed_item(
    buffer: ReadBuffer,
) -> tuple[ROOTSerializable, ReadBuffer]: ...


def read_streamed_item(
    buffer: ReadBuffer, method: Optional[ReadObjMethod] = None
) -> tuple[ROOTSerializable, ReadBuffer]:
    # Read ahead the stream header to determine the type of the object
    itemheader, _ = StreamHeader.read(buffer)
    if itemheader.fByteCount == 0 and itemheader.fClassRef is not None:
        # This is a reference to another object in the buffer
        if itemheader.fClassRef == 0:
            # Null reference, return None
            _, buffer = buffer.consume(4)
            return Ref(None), buffer
        # TODO: fetch the referenced object from the buffer.instance_refs
        _, buffer = buffer.consume(4)
        return Ref(None), buffer
    if itemheader.fClassName:
        clsname = normalize(itemheader.fClassName)
        if clsname not in DICTIONARY:
            if clsname == "TLeafI":
                msg = "TLeafI not declared in StreamerInfo, e.g. uproot-issue413.root"
                # (84 other test files have it, e.g. uproot-issue121.root)
                # https://github.com/scikit-hep/uproot3/issues/413
                # Likely groot-v0.21.0 (Go ROOT file implementation) did not write the streamers for TLeaf
                raise NotImplementedError(msg)
            if clsname == "RooRealVar":
                msg = "RooRealVar not declared in the StreamerInfo, e.g. uproot-issue49.root"
                raise NotImplementedError(msg)
            msg = f"Unknown class name: {itemheader.fClassName}"
            msg += f"\nStreamHeader: {itemheader}"
            raise ValueError(msg)
        dynmethod = DICTIONARY[clsname].read
    elif method is not None:
        clsname = f"Ref ({method})"

        def dynmethod(buffer: ReadBuffer) -> tuple[ROOTSerializable, ReadBuffer]:
            item, buffer = method(buffer)
            return Ref(item), buffer
    else:
        msg = f"StreamHeader has no class name: {itemheader}"
        raise ValueError(msg)

    # Now actually read the object
    item_end = itemheader.fByteCount + 4
    buffer, remaining = buffer[:item_end], buffer[item_end:]
    item, buffer = dynmethod(buffer)
    # TODO: register the object addr in the buffer instance_refs
    if buffer:
        msg = f"Expected buffer to be empty after reading {clsname}, but got\n{buffer}"
        raise ValueError(msg)
    return item, remaining


def _auto_TObject_base(buffer) -> tuple[StreamHeader, ReadBuffer]:
    """Deduce whether the TObject base class has a StreamHeader or not.
    This is the case for early versions of ROOT files.
    """
    (version,), _ = buffer.unpack(">h")
    if version < 0x40:
        itemheader = StreamHeader(0, version, None, None, False)
    else:
        itemheader, buffer = StreamHeader.read(buffer)
    return itemheader, buffer


@serializable
class StreamedObject(ROOTSerializable):
    """Base class for all streamed objects in ROOT."""

    @classmethod
    def read(cls, buffer: ReadBuffer):
        members, buffer = _read_all_members(cls, buffer)
        return cls(**members), buffer


def _read_all_members(
    cls: type[StreamedObject], buffer: ReadBuffer, indent=0
) -> tuple[Members, ReadBuffer]:
    start_position = buffer.relpos
    if cls.__name__ == "TObject" and indent > 0:
        itemheader, buffer = _auto_TObject_base(buffer)
    elif indent > 0 and getattr(cls, "_SkipHeader", False):
        itemheader = StreamHeader(0, None, None, None, False)
    else:
        itemheader, buffer = StreamHeader.read(buffer)
        if itemheader.fByteCount == 0:
            if cls.__name__ in ("TH1D", "TH2D"):
                msg = "Suspicious THx with fByteCount == 0 (e.g. uproot-issue-250.root)"
                raise NotImplementedError(msg)
            msg = "fByteCount is 0"
            raise ValueError(msg)
        if (
            itemheader.fClassName
            and normalize(itemheader.fClassName) != cls.__name__
            and not cls.__name__.startswith("TLeaf")
        ):
            msg = f"Expected class {cls.__name__} but got {normalize(itemheader.fClassName)}"
            raise ValueError(msg)
    end_position = start_position + itemheader.fByteCount + 4
    members: Members = {}
    for base in reversed(cls.__bases__):
        if base is StreamedObject:
            continue
        if issubclass(base, StreamedObject):
            base_members, buffer = _read_all_members(base, buffer, indent + 1)
            members.update(base_members)
        elif issubclass(base, ROOTSerializable):
            members, buffer = base.update_members(members, buffer)
    members, buffer = cls.update_members(members, buffer)
    if indent == 0 and buffer.relpos != end_position:
        # TODO: figure out why this does not hold in the subclasses (indent > 0)
        msg = f"Expected position {end_position} but got {buffer.relpos}"
        msg += f"\nClass: {cls}"
        msg += f"\nMembers: {members}"
        msg += f"\nBuffer: {buffer}"
        raise ValueError(msg)
    return members, buffer
