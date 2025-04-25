from typing import Annotated

from rootfilespec.bootstrap.streamedobject import read_streamed_item
from rootfilespec.bootstrap.strings import TString
from rootfilespec.bootstrap.TObject import TObject
from rootfilespec.buffer import ReadBuffer
from rootfilespec.dispatch import DICTIONARY
from rootfilespec.serializable import Members, ROOTSerializable, serializable
from rootfilespec.structutil import Fmt


@serializable
class TCollection(TObject):
    _SkipHeader = True
    fName: TString
    fSize: Annotated[int, Fmt(">i")]


DICTIONARY["TCollection"] = TCollection


@serializable
class TSeqCollection(TCollection):
    _SkipHeader = True


DICTIONARY["TSeqCollection"] = TSeqCollection


@serializable
class TList(TSeqCollection):
    """TList container class.
    Reference: https://root.cern/doc/master/streamerinfo.html (TList section)
    """

    items: tuple[TObject, ...]
    """List of objects."""

    @classmethod
    def update_members(cls, members: Members, buffer: ReadBuffer):
        items: list[TObject] = []
        fSize: int = members["fSize"]
        for _ in range(fSize):
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
        members["items"] = tuple(items)
        return members, buffer


DICTIONARY["TList"] = TList


@serializable
class TObjArray(TSeqCollection):
    """TObjArray container class."""

    fLowerBound: int
    """Lower bound of the array."""
    objects: tuple[ROOTSerializable, ...]
    """List of objects."""

    @classmethod
    def update_members(cls, members: Members, buffer: ReadBuffer):
        (members["fLowerBound"],), buffer = buffer.unpack(">i")
        fSize: int = members["fSize"]
        objects: list[ROOTSerializable] = []
        for _ in range(fSize):
            item, buffer = read_streamed_item(buffer)
            objects.append(item)
        members["objects"] = tuple(objects)
        return members, buffer


DICTIONARY["TObjArray"] = TObjArray
