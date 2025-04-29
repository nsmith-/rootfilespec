"""Types that appear to be assumed and not explicitly
present in the StreamerInfo dictionary.

TBasket and TArray* are also examples of this, but they are
implemented in their own files.
"""

from typing import Annotated

from rootfilespec.bootstrap.streamedobject import (
    StreamedObject,
    StreamHeader,
    read_streamed_item,
)
from rootfilespec.bootstrap.TObject import TObject
from rootfilespec.buffer import ReadBuffer
from rootfilespec.serializable import Members, ROOTSerializable, serializable
from rootfilespec.structutil import Fmt


@serializable
class TVirtualIndex(StreamedObject):
    @classmethod
    def update_members(cls, members: Members, buffer: ReadBuffer):
        raise NotImplementedError


@serializable
class TAtt3D(StreamedObject):
    """Some TH3D attributes

    This class is usually in the StreamerInfo, but missing in some files:
        uproot-from-geant4.root
        uproot-issue-250.root
    So we get "ValueError: Class TH3 depends on TAtt3D, which is not declared"
    Note that the second file also has a suspicious TH1D object with fByteCount == 0
    """


@serializable
class Uninterpreted(StreamedObject):
    """A class to represent an uninterpreted streamed object

    This is used for objects that are not recognized by the library.
    """

    header: StreamHeader
    """The header of the object."""
    data: bytes
    """The uninterpreted data of the object."""

    @classmethod
    def read(cls, buffer: ReadBuffer):
        header, _ = StreamHeader.read(buffer)
        data, buffer = buffer.consume(header.fByteCount + 4)
        return cls(header, data), buffer

    @classmethod
    def update_members(cls, members: Members, buffer: ReadBuffer):  # noqa: ARG003
        msg = "Logic error"
        raise RuntimeError(msg)


@serializable
class RooLinkedList(TObject):
    """The streamer for RooLinkedList (v3) appears to be incorrect"""

    _hashThresh: Annotated[int, Fmt(">h")]
    """Size threshold for hashing"""
    fSize: Annotated[int, Fmt(">i")]
    """Current size of list"""
    objects: tuple[ROOTSerializable, ...]

    @classmethod
    def read(cls, buffer: ReadBuffer):
        members, buffer = cls.update_members({}, buffer)
        return cls(**members), buffer

    @classmethod
    def update_members(cls, members: Members, buffer: ReadBuffer):
        members, buffer = TObject.update_members(members, buffer)
        (members["_hashThresh"], members["fSize"]), buffer = buffer.unpack(">hi")
        fSize: int = members["fSize"]
        objects: list[ROOTSerializable] = []
        for _ in range(fSize):
            item, buffer = read_streamed_item(buffer)
            objects.append(item)
        members["objects"] = tuple(objects)
        return members, buffer
