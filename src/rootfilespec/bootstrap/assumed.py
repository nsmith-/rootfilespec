"""Types that appear to be assumed and not explicitly
present in the StreamerInfo dictionary.

TBasket and TArray* are also examples of this, but they are
implemented in their own files.
"""

from typing import Optional

from rootfilespec.bootstrap.streamedobject import StreamedObject
from rootfilespec.buffer import ReadBuffer
from rootfilespec.dispatch import DICTIONARY
from rootfilespec.serializable import Members, ROOTSerializable, serializable


@serializable
class TVirtualIndex(StreamedObject):
    @classmethod
    def update_members(cls, members: Members, buffer: ReadBuffer):
        raise NotImplementedError


@serializable
class TAtt3D(ROOTSerializable):
    """Empty class for marking a TH1 as 3D"""


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
