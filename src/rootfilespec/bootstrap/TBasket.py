"""TBasket seems to be assumed in ROOT files and is not in the TStreamerInfo"""

from typing import Annotated

import numpy as np
from numpy.typing import NDArray

from rootfilespec.bootstrap.streamedobject import StreamHeader
from rootfilespec.bootstrap.TKey import TKey
from rootfilespec.buffer import ReadBuffer
from rootfilespec.dispatch import DICTIONARY
from rootfilespec.serializable import Members, ROOTSerializable, serializable
from rootfilespec.structutil import Fmt


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
    fEntryOffset: NDArray[np.int32]  #  BasicArray(np.dtype(">i"), "fNevBuf")
    "Offset of entries in fBuffer(TKey)"
    fBuffer: bytes
    "Buffer if the basket owns it"

    @classmethod
    def update_members(cls, members: Members, buffer: ReadBuffer):
        start_position = buffer.relpos
        sheader, buffer = StreamHeader.read(buffer)
        members, buffer = TKey.update_members(members, buffer)
        base_tkey = TKey(**members)
        bheader, buffer = TBasket_header.read(buffer)

        fEntryOffset: NDArray[np.int32] = np.full(
            bheader.fNevBuf, -1, dtype=np.dtype(">i")
        )
        fBuffer = b""
        # TODO: full flag handling
        # https://github.com/root-project/root/blob/0e6282a641b65bdf5ad832882e547ca990e8f1a5/tree/tree/src/TBasket.cxx#L993-L1027
        if bheader.flag not in (11, 12):
            msg = f"TBasket header flag {bheader.flag} not supported"
            raise ValueError(msg)
        if bheader.fNevBuf > 0 and bheader.flag % 10 != 2:
            # TODO: refactor this to use BasicArray
            (n,), buffer = buffer.unpack(">i")
            assert n == bheader.fNevBuf
            data, buffer = buffer.consume(n * fEntryOffset.dtype.itemsize)
            fEntryOffset = np.frombuffer(data, dtype=fEntryOffset.dtype, count=n)
        if base_tkey.header.is_embedded():
            _, buffer = TKey.update_members(
                {}, buffer
            )  # TODO: worth checking consistency?
            _, buffer = TBasket_header.read(buffer)
            end_position = start_position + sheader.fByteCount + 4
            fBuffer, buffer = buffer.consume(end_position - buffer.relpos)
        members["bheader"] = bheader
        members["fEntryOffset"] = fEntryOffset
        members["fBuffer"] = fBuffer
        return members, buffer


DICTIONARY["TBasket"] = TBasket
