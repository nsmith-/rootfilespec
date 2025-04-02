from __future__ import annotations

import warnings
from dataclasses import dataclass
from enum import IntEnum
from typing import Annotated, Any, TypeVar

from rootfilespec.bootstrap.TString import TString
from rootfilespec.dispatch import DICTIONARY, normalize
from rootfilespec.structutil import (
    Fmt,
    ReadBuffer,
    ROOTSerializable,
    serializable,
)


class _StreamConstants(IntEnum):
    """Constants used for data members of StreamedObject class.

    Attributes:
        kByteCountMask (int): Mask for ByteCount
        kClassMask (int): Mask for ClassInfo
        kNewClassTag (int): New class tag
        kNotAVersion (int): Either kClassMask or kNewClassTag is set in bytes 4-5
    """

    kByteCountMask = 0x40000000
    kClassMask = 0x80000000
    kNewClassTag = 0xFFFFFFFF
    kNotAVersion = 0x8000


@dataclass
class StreamHeader(ROOTSerializable):
    """Initial header for any streamed data object

    Reference: https://root.cern/doc/master/dobject.html

    Attributes:
        fByteCount (int): Number of remaining bytes in object (uncompressed)
        fVersion (int): Version of Class
        fClassName (bytes): Class name of object, if first instance of class in buffer
        fClassRef (int): Position in buffer of class name if not specified here

    Only one of fVersion, fClassName, or fClassRef will be set.
    """

    fByteCount: int
    fVersion: int | None
    fClassName: bytes | None
    fClassRef: int | None

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
                buffer.local_refs[fClassRef] = fClassName
            else:
                fClassRef = (fClassInfo & ~_StreamConstants.kClassMask) - 2
                fClassName = buffer.local_refs.get(fClassRef, None)
                if fClassName is None:
                    msg = f"ClassRef {fClassRef} not found in buffer local_refs"
                    raise ValueError(msg)
        return cls(fByteCount, fVersion, fClassName, fClassRef), buffer


class TObjectBits(IntEnum):
    """Bits for TObject class.

    Relevant bits for ROOTIO are:
        kCanDelete - if object in a list can be deleted.
        kMustCleanup - if other objects may need to be deleted when this one is.
        kIsReferenced - if object is referenced by pointer to persistent object.
        kZombie - if object ctor succeeded but object shouldn't be used
        kIsOnHeap - if object is on Heap.
        kNotDeleted - if object has not been deleted.
    """

    kCanDelete = 0x00000001
    kIsOnHeap = 0x01000000
    kIsReferenced = 0x00000010
    kMustCleanup = 0x00000008
    kNotDeleted = 0x02000000
    kZombie = 0x00002000
    kNotSure = 0x00010000


T = TypeVar("T", bound="StreamedObject")


@serializable
class StreamedObject(ROOTSerializable):
    @classmethod
    def read(cls: type[T], buffer: ReadBuffer) -> tuple[T, ReadBuffer]:
        args, buffer = cls._read_all_members(buffer)
        return cls(*args), buffer

    @classmethod
    def _read_all_members(
        cls: type[T], buffer: ReadBuffer, indent=0
    ) -> tuple[tuple[Any, ...], ReadBuffer]:
        # TODO move this to a free function
        start_position = buffer.relpos
        itemheader, buffer = StreamHeader.read(buffer)
        # TODO: this is not the right place for version-specific logic
        if cls is TNamed and itemheader.fVersion == 1:
            # Early versions don't have the TObject stream header
            args0, buffer = TObject.read_members(buffer)
            args1, buffer = TNamed.read_members(buffer)
            return (args0 + args1), buffer
        if cls.__name__ == "TObjString" and itemheader.fVersion == 1:
            args0, buffer = TObject.read_members(buffer)
            args1, buffer = TString.read_members(buffer)
            return (args0 + args1), buffer
        if cls.__name__ == "TObjArray" and itemheader.fVersion == 3:
            args0, buffer = TObject.read_members(buffer)
            args1, buffer = cls.read_members(buffer)
            return (args0 + args1), buffer
        if itemheader.fClassName and normalize(itemheader.fClassName) != cls.__name__:
            msg = f"Expected class {cls.__name__} but got {itemheader.fClassName}"
            raise ValueError(msg)
        end_position = start_position + itemheader.fByteCount + 4
        args = ()
        for base in reversed(cls.__bases__):
            if issubclass(base, StreamedObject) and base is not StreamedObject:
                base_args, buffer = base._read_all_members(buffer, indent + 1)
                args += base_args
        try:
            cls_args, buffer = cls.read_members(buffer)
        except NotImplementedError:
            if indent != 0:
                # we only know how to skip forward for indent = 0
                raise
            uninterpreted, buffer = buffer.consume(end_position - buffer.relpos)
            cls_args = (uninterpreted,)
            warnings.warn(
                f"Class {cls} does not implement read_members, skipping data",
                stacklevel=1,
            )
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

    Attributes:
        header (TObject_header): Header data for TObject class.
        pidf (int): An identifier of the TProcessID record for the process that wrote the
            object. This identifier is an unsigned short. The relevant record
            has a name that is the string "ProcessID" concatenated with the ASCII
            decimal representation of "pidf" (no leading zeros). 0 is a valid pidf.
            Only present if the object is referenced by a pointer to persistent object.
    """

    fVersion: Annotated[int, Fmt(">h")]
    fUniqueID: Annotated[int, Fmt(">i")]
    fBits: Annotated[int, Fmt(">i")]
    pidf: int | None

    @classmethod
    def read_members(cls, buffer: ReadBuffer) -> tuple[tuple[Any, ...], ReadBuffer]:
        (fVersion, fUniqueID, fBits), buffer = buffer.unpack(">hii")
        pidf = None
        if fBits & TObjectBits.kIsReferenced:
            (pidf,), buffer = buffer.unpack(">H")
        return (fVersion, fUniqueID, fBits, pidf), buffer


DICTIONARY["TObject"] = TObject


@serializable
class TNamed(TObject):
    """Format for TNamed class.

    Attributes:
        b_object (TObject): TObject base class.
        fName (TString): Name of the object.
        fTitle (TString): Title of the object.
    """

    fName: TString
    fTitle: TString


DICTIONARY["TNamed"] = TNamed
