"""Types that appear to be assumed and not explicitly
present in the StreamerInfo dictionary.

TBasket and TArray* are also examples of this, but they are
implemented in their own files.
"""

from typing import Optional

from rootfilespec.bootstrap.streamedobject import StreamedObject, StreamHeader
from rootfilespec.buffer import ReadBuffer
from rootfilespec.dispatch import DICTIONARY
from rootfilespec.serializable import Members, serializable


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
class ROOT3a3aTIOFeatures(StreamedObject):
    fIOBits: int
    extra: Optional[int]

    @classmethod
    def update_members(cls, members: Members, buffer: ReadBuffer):
        (fIOBits,), buffer = buffer.unpack(">B")
        extra: Optional[int] = None
        if fIOBits > 0:
            # TODO: why is this 4 bytes here?
            (extra,), buffer = buffer.unpack(">i")
        members["fIOBits"] = fIOBits
        members["extra"] = extra
        return members, buffer


DICTIONARY["ROOT3a3aTIOFeatures"] = ROOT3a3aTIOFeatures


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
        header, buffer = StreamHeader.read(buffer)
        data, buffer = buffer.consume(header.fByteCount - 4)
        return cls(header, data), buffer

    @classmethod
    def update_members(cls, members: Members, buffer: ReadBuffer):  # noqa: ARG003
        msg = "Logic error"
        raise RuntimeError(msg)
