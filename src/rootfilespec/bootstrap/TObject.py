from enum import IntEnum
from typing import Annotated, Optional

from rootfilespec.bootstrap.streamedobject import StreamedObject
from rootfilespec.bootstrap.strings import TString
from rootfilespec.buffer import ReadBuffer
from rootfilespec.dispatch import DICTIONARY
from rootfilespec.serializable import Members, serializable
from rootfilespec.structutil import Fmt


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
    def update_members(
        cls, members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        (fVersion, fUniqueID, fBits), buffer = buffer.unpack(">hii")
        pidf = None
        if fBits & TObjectBits.kIsReferenced:
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
