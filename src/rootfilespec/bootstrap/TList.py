from __future__ import annotations

from dataclasses import dataclass

from rootfilespec.bootstrap.streamedobject import read_streamed_item
from rootfilespec.bootstrap.TObject import StreamHeader, TObject, TObjectBits
from rootfilespec.bootstrap.TString import TString
from rootfilespec.dispatch import DICTIONARY
from rootfilespec.structutil import ReadBuffer, serializable


@dataclass
class TList(TObject):
    """TList container class.
    Reference: https://root.cern/doc/master/streamerinfo.html (TList section)
    """

    fName: TString
    """Name of the list."""
    fN: int
    """Number of objects in the list."""
    items: list[TObject]
    """List of objects."""

    @classmethod
    def read(cls, buffer: ReadBuffer):
        header, buffer = StreamHeader.read(buffer)
        (fVersion, fUniqueID, fBits, pidf), buffer = TObject.read_members(buffer)
        if fVersion == 1 << 14 and (fBits & TObjectBits.kNotSure):
            # This looks like schema evolution data
            # print(f"Suspicious TObject header: {header}")
            # print(f"Buffer: {buffer}")
            junk, buffer = buffer.consume(len(buffer) - 1)
            return cls(fVersion, fUniqueID, fBits, pidf, TString(junk), 0, []), buffer
        (fName, fN, items), buffer = cls.read_members(buffer)
        return cls(fVersion, fUniqueID, fBits, pidf, fName, fN, items), buffer

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        fName, buffer = TString.read(buffer)
        (fN,), buffer = buffer.unpack(">i")
        items: list[TObject] = []
        for _ in range(fN):
            item, buffer = read_streamed_item(buffer)
            if not isinstance(item, TObject):
                msg = f"Expected TObject but got {item!r}"
                raise ValueError(msg)
            # No idea why there is a null pad byte here
            pad, buffer = buffer.consume(1)
            if pad != b"\x00":
                msg = f"Expected null pad byte but got {pad!r}"
                raise ValueError(msg)
            items.append(item)
        return (fName, fN, items), buffer


DICTIONARY["TList"] = TList


@serializable
class TObjArray(TObject):
    """TObjArray container class."""

    fName: TString
    """Name of the array."""
    nObjects: int
    """Number of objects in the array."""
    fLowerBound: int
    """Lower bound of the array."""
    objects: list[TObject]
    """List of objects."""

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        fName, buffer = TString.read(buffer)
        (nObjects, fLowerBound), buffer = buffer.unpack(">ii")
        objects: list[TObject] = []
        for _ in range(nObjects):
            item, buffer = read_streamed_item(buffer)
            if isinstance(item, StreamHeader):
                # TODO: Resolve or build pointer to TObject
                continue
            if not isinstance(item, TObject):
                msg = f"Expected TObject but got {item!r}"
                raise ValueError(msg)
            objects.append(item)
        return (fName, nObjects, fLowerBound, objects), buffer


DICTIONARY["TObjArray"] = TObjArray
