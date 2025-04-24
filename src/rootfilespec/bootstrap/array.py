from typing import Annotated

from rootfilespec.structutil import (
    ReadBuffer,
    ROOTSerializable,
    serializable,
)

# TODO: template these classes


@serializable
class TArrayC(ROOTSerializable):
    fN: Annotated[int, ">i"]
    fA: Annotated[list[int], ">B"]

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        (n,), buffer = buffer.unpack(">i")
        a, buffer = buffer.unpack(f">{n}B")
        return (n, list(a)), buffer


@serializable
class TArrayS(ROOTSerializable):
    fN: Annotated[int, ">i"]
    fA: Annotated[list[int], ">h"]

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        (n,), buffer = buffer.unpack(">i")
        a, buffer = buffer.unpack(f">{n}h")
        return (n, list(a)), buffer


@serializable
class TArrayI(ROOTSerializable):
    fN: Annotated[int, ">i"]
    fA: Annotated[list[int], ">i"]

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        (n,), buffer = buffer.unpack(">i")
        a, buffer = buffer.unpack(f">{n}i")
        return (n, list(a)), buffer


@serializable
class TArrayF(ROOTSerializable):
    fN: Annotated[int, ">i"]
    fA: Annotated[list[float], ">f"]

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        (n,), buffer = buffer.unpack(">i")
        a, buffer = buffer.unpack(f">{n}f")
        return (n, list(a)), buffer


@serializable
class TArrayD(ROOTSerializable):
    fN: Annotated[int, ">i"]
    fA: Annotated[list[float], ">d"]

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        (n,), buffer = buffer.unpack(">i")
        a, buffer = buffer.unpack(f">{n}d")
        return (n, list(a)), buffer
