from rootfilespec.bootstrap.streamedobject import read_streamed_item
from rootfilespec.bootstrap.strings import TString
from rootfilespec.bootstrap.TObject import TObject, TObjectBits
from rootfilespec.buffer import ReadBuffer
from rootfilespec.dispatch import DICTIONARY
from rootfilespec.serializable import Members, ROOTSerializable, serializable


@serializable
class TList(TObject):
    """TList container class.
    Reference: https://root.cern/doc/master/streamerinfo.html (TList section)
    """

    fName: TString
    """Name of the list."""
    fN: int
    """Number of objects in the list."""
    items: tuple[TObject, ...]
    """List of objects."""

    @classmethod
    def update_members(cls, members: Members, buffer: ReadBuffer):
        base_tobject = TObject(**members)
        if base_tobject.fVersion == 1 << 14 and (
            base_tobject.fBits & TObjectBits.kNotSure
        ):
            # This looks like schema evolution data
            raise ValueError()
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
        members["fName"] = fName
        members["fN"] = fN
        members["items"] = tuple(items)
        return members, buffer


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
    objects: tuple[ROOTSerializable, ...]
    """List of objects."""

    @classmethod
    def update_members(cls, members: Members, buffer: ReadBuffer):
        fName, buffer = TString.read(buffer)
        (nObjects, fLowerBound), buffer = buffer.unpack(">ii")
        objects: list[ROOTSerializable] = []
        for _ in range(nObjects):
            item, buffer = read_streamed_item(buffer)
            objects.append(item)
        members["fName"] = fName
        members["nObjects"] = nObjects
        members["fLowerBound"] = fLowerBound
        members["objects"] = tuple(objects)
        return members, buffer


DICTIONARY["TObjArray"] = TObjArray
