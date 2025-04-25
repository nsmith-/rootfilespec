"""Types that appear to be assumed and not explicitly
present in the StreamerInfo dictionary.

TBasket and TArray* are also examples of this, but they are
implemented in their own files.
"""

from typing import Annotated

from rootfilespec.bootstrap.TObject import StreamedObject
from rootfilespec.dispatch import DICTIONARY
from rootfilespec.serializable import serializable
from rootfilespec.structutil import (
    Fmt,
    Members,
    ReadBuffer,
    ROOTSerializable,
)


@serializable
class TVirtualIndex(ROOTSerializable):
    uninterpreted: bytes

    @classmethod
    def update_members(cls, members: Members, buffer: ReadBuffer):
        raise NotImplementedError


@serializable
class TAtt3D(ROOTSerializable):
    """Empty class for marking a TH1 as 3D"""


@serializable
class ROOT3a3aTIOFeatures(StreamedObject):
    fIOBits: Annotated[int, Fmt(">B")]

    @classmethod
    def update_members(cls, members: Members, buffer: ReadBuffer):
        # TODO: why is this 4 bytes here?
        junk, buffer = buffer.unpack(">i")
        (fIOBits,), buffer = buffer.unpack(">B")
        members["fIOBits"] = fIOBits
        return members, buffer


DICTIONARY["ROOT3a3aTIOFeatures"] = ROOT3a3aTIOFeatures
