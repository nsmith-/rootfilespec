from dataclasses import dataclass
from enum import IntEnum
from typing import Annotated, Optional, TypeVar

from rootfilespec.bootstrap.TString import TString
from rootfilespec.dispatch import DICTIONARY, normalize
from rootfilespec.structutil import (
    Args,
    Fmt,
    ReadBuffer,
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

    @classmethod
    def read(cls, buffer: ReadBuffer):
        # read all we might need in one go and advance later (optimization)
        (fByteCount, tmp1, tmp2), _ = buffer.unpack(">iHH")
        if fByteCount & _StreamConstants.kByteCountMask:
            fByteCount &= ~_StreamConstants.kByteCountMask
        else:
            # This is a reference to another object in the buffer
            _, buffer = buffer.consume(4)
            return cls(0, None, None, fByteCount), buffer
        fVersion, fClassName, fClassRef = None, None, None
        if not (tmp1 & _StreamConstants.kNotAVersion):
            fVersion = tmp1
            _, buffer = buffer.consume(6)  # now advance the buffer
        else:
            fVersion = None
            fClassInfo: int = (tmp1 << 16) | tmp2
            _, buffer = buffer.consume(8)  # now advance the buffer
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
                    msg = f"ClassRef {fClassRef} not found in buffer local_refs"
                    raise ValueError(msg)
        return cls(fByteCount, fVersion, fClassName, fClassRef), buffer


class TObjectBits(IntEnum):
    """Bits for TObject class."""

    kCanDelete = 0x00000001
    """If object in a list can be deleted."""
    kIsOnHeap = 0x01000000
    """If object is on Heap."""
    kIsReferenced = 0x00000010
    """If object is referenced by pointer to persistent object."""
    kMustCleanup = 0x00000008
    """If other objects may need to be deleted when this one is."""
    kNotDeleted = 0x02000000
    """If object has not been deleted."""
    kZombie = 0x00002000
    """If object ctor succeeded but object shouldn't be used."""
    kNotSure = 0x00010000
    """If object is not sure if it is on heap or not."""


T = TypeVar("T", bound="StreamedObject")


def _auto_TObject_base(buffer) -> tuple[StreamHeader, ReadBuffer]:
    """Deduce whether the TObject base class has a StreamHeader or not.
    This is the case for early versions of ROOT files.
    """
    (version,), _ = buffer.unpack(">h")
    if version < 0x40:
        itemheader = StreamHeader(0, version, None, None)
    else:
        itemheader, buffer = StreamHeader.read(buffer)
    return itemheader, buffer


@serializable
class StreamedObject(ROOTSerializable):
    """Base class for all streamed objects in ROOT."""

    @classmethod
    def read(cls: type[T], buffer: ReadBuffer) -> tuple[T, ReadBuffer]:
        args, buffer = cls._read_all_members(buffer)
        return cls(*args), buffer

    @classmethod
    def _read_all_members(
        cls: type[T], buffer: ReadBuffer, indent=0
    ) -> tuple[Args, ReadBuffer]:
        # TODO move this to a free function
        start_position = buffer.relpos
        if cls is TObject and indent > 0:
            itemheader, buffer = _auto_TObject_base(buffer)
        else:
            itemheader, buffer = StreamHeader.read(buffer)
        if itemheader.fClassName and normalize(itemheader.fClassName) != cls.__name__:
            msg = f"Expected class {cls.__name__} but got {normalize(itemheader.fClassName)}"
            raise ValueError(msg)
        end_position = start_position + itemheader.fByteCount + 4
        args: Args = ()
        for base in reversed(cls.__bases__):
            if base is StreamedObject:
                continue
            if issubclass(base, StreamedObject):
                base_args, buffer = base._read_all_members(buffer, indent + 1)
                args += base_args
            elif issubclass(base, ROOTSerializable):
                base_args, buffer = base.read_members(buffer)
                args += base_args
        cls_args, buffer = cls.read_members(buffer)
        args += cls_args
        if indent == 0 and buffer.relpos != end_position:
            # TODO: figure out why this does not hold in the subclasses (indent > 0)
            msg = f"Expected position {end_position} but got {buffer.relpos}"
            msg += f"\nClass: {cls}"
            msg += f"\nArgs: {args}"
            msg += f"\nBuffer: {buffer}"
            raise ValueError(msg)
        return args, buffer


@serializable
class TObject(StreamedObject):
    """Format for TObject class.
    Reference: https://root.cern/doc/master/tobject.html
    """

    fVersion: Annotated[int, Fmt(">h")]
    """Version of the class."""
    fUniqueID: Annotated[int, Fmt(">i")]
    """Unique ID of the object."""
    fBits: Annotated[int, Fmt(">i")]
    """Bit mask for the object."""
    pidf: Optional[int]
    """An identifier of the TProcessID record for the process that wrote the object.
    This identifier is an unsigned short. The relevant record has a name that is
    the string "ProcessID" concatenated with the ASCII decimal representation of
    "pidf" (no leading zeros). 0 is a valid pidf. Only present if the object
    is referenced by a pointer to persistent object."""

    @classmethod
    def read_members(cls, buffer: ReadBuffer) -> tuple[Args, ReadBuffer]:
        (fVersion, fUniqueID, fBits), buffer = buffer.unpack(">hii")
        pidf = None
        if fBits & TObjectBits.kIsReferenced:
            (pidf,), buffer = buffer.unpack(">H")
        return (fVersion, fUniqueID, fBits, pidf), buffer


DICTIONARY["TObject"] = TObject


@serializable
class TNamed(TObject):
    """Format for TNamed class."""

    fName: TString
    """Name of the object."""
    fTitle: TString
    """Title of the object."""


DICTIONARY["TNamed"] = TNamed
