"""Types that appear to be assumed and not explicitly
present in the StreamerInfo dictionary.
"""

from __future__ import annotations

from typing import Annotated

from rootfilespec.bootstrap.TObject import StreamedObject
from rootfilespec.dispatch import DICTIONARY
from rootfilespec.structutil import Fmt, ReadBuffer, ROOTSerializable, serializable


@serializable
class TArrayI(ROOTSerializable):
    fN: Annotated[int, ">i"]
    fA: Annotated[list[int], ">i"]

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        (n,), buffer = buffer.unpack(">i")
        if n > 0:
            raise NotImplementedError
        a, buffer = buffer.unpack(f">{n}i")
        return (n, list(a)), buffer


@serializable
class TArrayD(ROOTSerializable):
    fN: Annotated[int, ">i"]
    fA: Annotated[list[float], ">d"]

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        (n,), buffer = buffer.unpack(">i")
        if n > 0:
            raise NotImplementedError
        a, buffer = buffer.unpack(f">{n}d")
        return (n, list(a)), buffer


@serializable
class TVirtualIndex(ROOTSerializable):
    uninterpreted: bytes

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        raise NotImplementedError


@serializable
class ROOT3a3aTIOFeatures(StreamedObject):
    fIOBits: Annotated[int, Fmt(">B")]

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        # TODO: why is this 4 bytes here?
        junk, buffer = buffer.unpack(">i")
        (fIOBits,), buffer = buffer.unpack(">B")
        return (fIOBits,), buffer


DICTIONARY["ROOT3a3aTIOFeatures"] = ROOT3a3aTIOFeatures
