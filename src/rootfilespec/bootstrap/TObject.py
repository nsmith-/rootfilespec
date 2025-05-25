from enum import IntFlag
from typing import Annotated, Optional

from rootfilespec.bootstrap.streamedobject import StreamedObject
from rootfilespec.bootstrap.strings import TString
from rootfilespec.buffer import ReadBuffer
from rootfilespec.dispatch import DICTIONARY
from rootfilespec.serializable import Members, serializable
from rootfilespec.structutil import Fmt


class TObjFlag(IntFlag):
    """Bits for TObject class.

    https://github.com/root-project/root/blob/b5ac9cdad06a41a4a3e18d568b4198872f388e92/core/base/inc/TObject.h#L63C12-L63C77

    Global bits (can be set for any object and should not be reused).
    Bits 0 - 13 are reserved as global bits. Bits 14 - 23 can be used in different class hierarchies
    (make sure there is no overlap in any given hierarchy).
    """

    # Private bits
    kIsOnHeap = 0x01000000
    """If object is on Heap."""
    kNotDeleted = 0x02000000
    """If object has not been deleted."""
    kZombie = 0x04000000
    """If object constructor failed."""
    kInconsistent = 0x08000000
    """If class overloads Hash but does not call RecursiveRemove in destructor."""

    kCanDelete = 0x00000001
    """If object in a list can be deleted."""
    kMustCleanup = 0x00000008
    """If other objects may need to be deleted when this one is."""
    kIsReferenced = 0x00000010
    """If object is referenced by pointer to persistent object."""
    kHasUUID = 0x00000020
    """If object has a TUUID (its fUniqueID=UUIDNumber)"""
    kCannotPick = 0x00000040
    """If object in a pad cannot be picked"""
    # 7 is taken by TAxis and TClass.
    kNotSure = 0x10000
    """If object is not sure if it is on heap or not."""
    kMysteryStreamer = 0x20000
    """Appears in some TStreamerElement"""
    kMysteryStreamer2 = 0x1000
    """Appears in some TStreamerElement"""
    kDoNotDelete = 0x2000
    """TStreamerElement status bit or kInvalidObject"""
    kListOfRules = 0x4000
    """The schema evolution rules stored at the end of the streamer info"""

    # TODO: validate on initialization that no bits are set that are not defined in this class
    def __repr__(self):
        setbits: list[str] = []
        val = int(self)
        for bit in TObjFlag:
            if val & bit:
                setbits.append(self.__class__.__name__ + "." + (bit.name or "UNKNOWN"))
                val &= ~bit
        if val:
            msg = f"Invalid bits set in {self.__class__.__name__}: {val:#x}"
            raise ValueError(msg)
        return f"{self.__class__.__name__}({'|'.join(setbits)})"


@serializable
class TObject(StreamedObject):
    """Format for TObject class.
    Reference: https://root.cern/doc/master/tobject.html
    """

    fVersion: Annotated[int, Fmt(">h")]
    """Version of the class."""
    fUniqueID: Annotated[int, Fmt(">i")]
    """Unique ID of the object."""
    fBits: Annotated[TObjFlag, Fmt(">i")]
    """Bit mask for the object."""
    pidf: Optional[int]
    """An identifier of the TProcessID record for the process that wrote the object.
    This identifier is an unsigned short. The relevant record has a name that is
    the string "ProcessID" concatenated with the ASCII decimal representation of
    "pidf" (no leading zeros). 0 is a valid pidf. Only present if the object
    is referenced by a pointer to persistent object."""

    @classmethod
    def update_members(
        cls, members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        (fVersion, fUniqueID, fBits), buffer = buffer.unpack(">hii")
        fBits = TObjFlag(fBits)
        pidf = None
        if fBits & TObjFlag.kIsReferenced:
            (pidf,), buffer = buffer.unpack(">H")
        members["fVersion"] = fVersion
        members["fUniqueID"] = fUniqueID
        members["fBits"] = fBits
        members["pidf"] = pidf
        return members, buffer


DICTIONARY["TObject"] = TObject


@serializable
class TObjString(TObject):
    """Format for TObjString class."""

    fString: TString
    """String data of the object."""


DICTIONARY["TObjString"] = TObjString


@serializable
class TNamed(TObject):
    """Format for TNamed class."""

    fName: TString
    """Name of the object."""
    fTitle: TString
    """Title of the object."""


DICTIONARY["TNamed"] = TNamed
