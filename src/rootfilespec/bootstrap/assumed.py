"""Types that appear to be assumed and not explicitly
present in the StreamerInfo dictionary.
"""

from typing import Annotated, Any

import numpy as np

from rootfilespec.bootstrap.TKey import TKey
from rootfilespec.bootstrap.TObject import StreamedObject, StreamHeader
from rootfilespec.dispatch import DICTIONARY
from rootfilespec.structutil import (
    Fmt,
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


@serializable
class TVirtualIndex(ROOTSerializable):
    uninterpreted: bytes

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        raise NotImplementedError


@serializable
class TAtt3D(ROOTSerializable):
    """Empty class for marking a TH1 as 3D"""


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


@serializable
class TBasket_header(ROOTSerializable):
    fVersion: Annotated[int, Fmt(">h")]
    "Version of the TBasket class"
    fBufferSize: Annotated[int, Fmt(">i")]
    "fBuffer length in bytes"
    fNevBufSize: Annotated[int, Fmt(">i")]
    "Length in Int_t of fEntryOffset OR fixed length of each entry if fEntryOffset is null!"
    fNevBuf: Annotated[int, Fmt(">i")]
    "Number of entries in basket"
    fLast: Annotated[int, Fmt(">i")]
    "Pointer to last used byte in basket"
    flag: Annotated[int, Fmt(">B")]
    """Some flags that control what is read/written:
        - flag % 10 !=2 means fEntryOffset is available
    """
    # fIOBits: Annotated[int, Fmt(">B")]
    # "IO feature flags.  Serialized in custom portion of streamer to avoid forward compat issues unless needed."


@serializable
class TBasket(TKey):
    """TBasket object"""

    bheader: TBasket_header
    "Header of the TBasket"
    fEntryOffset: np.ndarray[
        Any, np.dtypes.Int32DType
    ]  #  BasicArray(np.dtype(">i"), "fNevBuf")
    "Offset of entries in fBuffer(TKey)"
    fBuffer: bytes
    "Buffer if the basket owns it"

    @classmethod
    def read(cls, buffer: ReadBuffer):
        args, buffer = cls.read_members(buffer)
        return cls(*args), buffer

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        start_position = buffer.relpos
        sheader, buffer = StreamHeader.read(buffer)
        keyargs, buffer = TKey.read_members(buffer)
        keyheader = keyargs[0]
        bheader, buffer = TBasket_header.read(buffer)

        fEntryOffset = np.full(bheader.fNevBuf, -1, dtype=">i")
        fBuffer = b""
        # TODO: full flag handling
        # https://github.com/root-project/root/blob/0e6282a641b65bdf5ad832882e547ca990e8f1a5/tree/tree/src/TBasket.cxx#L993-L1027
        if bheader.flag not in (11, 12):
            msg = f"TBasket header flag {bheader.flag} not supported"
            raise ValueError(msg)
        if bheader.flag % 10 != 2:
            # TODO: refactor this to use BasicArray
            dtype = np.dtype(">i")  # type: ignore[var-annotated]
            (n,), buffer = buffer.unpack(">i")
            assert n == bheader.fNevBuf
            data, buffer = buffer.consume(n * dtype.itemsize)
            fEntryOffset = np.frombuffer(data, dtype=dtype, count=n)
        if keyheader.is_embedded():
            _, buffer = TKey.read_members(buffer)
            _, buffer = TBasket_header.read(buffer)
            end_position = start_position + sheader.fByteCount + 4
            fBuffer, buffer = buffer.consume(end_position - buffer.relpos)
        return (*keyargs, bheader, fEntryOffset, fBuffer), buffer


DICTIONARY["TBasket"] = TBasket
