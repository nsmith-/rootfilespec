from __future__ import annotations

from typing import Annotated, TypeVar, overload

from rootfilespec.bootstrap.compression import RCompressed
from rootfilespec.bootstrap.TString import TString
from rootfilespec.bootstrap.util import fDatime_to_datetime
from rootfilespec.dispatch import DICTIONARY, normalize
from rootfilespec.structutil import (
    DataFetcher,
    Fmt,
    ReadBuffer,
    ROOTSerializable,
    serializable,
)


@serializable
class TKey_header(ROOTSerializable):
    """TKey header information"""

    fNbytes: Annotated[int, Fmt(">i")]
    """Number of bytes in compressed record (Tkey+data)"""
    fVersion: Annotated[int, Fmt(">h")]
    """TKey class version identifier"""
    fObjlen: Annotated[int, Fmt(">i")]
    """Number of bytes of uncompressed data"""
    fDatime: Annotated[int, Fmt(">i")]
    """Date and time when record was written to file"""
    fKeylen: Annotated[int, Fmt(">h")]
    """Number of bytes in key structure (TKey)"""
    fCycle: Annotated[int, Fmt(">h")]
    """Cycle of key"""

    def write_time(self):
        """Date and time when record was written to file"""
        return fDatime_to_datetime(self.fDatime)


ObjType = TypeVar("ObjType", bound=ROOTSerializable)


@serializable
class TKey(ROOTSerializable):
    """TKey object.
    See https://root.cern/doc/master/classTKey.html for more information.
    """

    header: TKey_header
    """TKey header information"""
    fSeekKey: int
    """Byte offset of record itself (consistency check)"""
    fSeekPdir: int
    """Byte offset of parent directory record"""
    fClassName: TString
    """Object Class Name"""
    fName: TString
    """Name of the object"""
    fTitle: TString
    """Title of the object"""

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        initial_size = len(buffer)
        header, buffer = TKey_header.read(buffer)
        if header.fVersion < 1000:
            (fSeekKey, fSeekPdir), buffer = buffer.unpack(">ii")
        else:
            (fSeekKey, fSeekPdir), buffer = buffer.unpack(">qq")
        fClassName, buffer = TString.read(buffer)
        fName, buffer = TString.read(buffer)
        fTitle, buffer = TString.read(buffer)
        if len(buffer) != initial_size - header.fKeylen:
            raise ValueError("TKey.read: buffer size mismatch")  # noqa: EM101
        return (header, fSeekKey, fSeekPdir, fClassName, fName, fTitle), buffer

    def is_short(self) -> bool:
        """Return if the key is short (i.e. the seeks are 32 bit)"""
        return self.header.fVersion < 1000

    @overload
    def read_object(self, fetch_data: DataFetcher) -> ROOTSerializable: ...

    @overload
    def read_object(
        self, fetch_data: DataFetcher, objtype: type[ObjType]
    ) -> ObjType: ...

    def read_object(
        self,
        fetch_data: DataFetcher,
        objtype: type[ObjType] | None = None,
    ) -> ObjType | ROOTSerializable:
        buffer = fetch_data(
            self.fSeekKey + self.header.fKeylen,
            self.header.fNbytes - self.header.fKeylen,
        )

        compressed = None
        # fObjlen is the number of bytes of uncompressed data
        # The length of the buffer is the number of bytes of compressed data
        if len(buffer) != self.header.fObjlen:
            # This is a compressed object
            compressed, buffer = RCompressed.read(buffer)
            if compressed.header.uncompressed_size() != self.header.fObjlen:
                msg = "TKey.read_object: uncompressed size mismatch. "
                msg += (
                    f"{compressed.header.uncompressed_size()} != {self.header.fObjlen}"
                )
                msg += "\nThis might be expected for very large TBaskets"

                raise ValueError(msg)
            if buffer:
                msg = f"TKey.read_object: buffer not empty after reading compressed object. {buffer=}"
                raise ValueError(msg)
            buffer = ReadBuffer(
                compressed.decompress(),
                abspos=None,
                relpos=self.header.fKeylen,
            )
        if objtype is not None:
            typename = objtype.__name__
            # if self.fClassName.fString != typename:
            #     msg = f"TKey.read_object: type mismatch: expected {typename!r} but got {self.fClassName.fString!r}"
            #     raise ValueError(msg)
            obj, buffer = objtype.read(buffer)
        else:
            typename = normalize(self.fClassName.fString)
            obj, buffer = DICTIONARY[typename].read(buffer)  # type: ignore[assignment]
        if typename == "ROOT3a3aRNTuple":
            # RNTuple is a special case, it has a 64 bit checksum
            (checksum,), buffer = buffer.unpack(">Q")
            # TODO: Check the checksum
        if buffer:
            msg = f"TKey.read_object: buffer not empty after reading object of type {typename}."
            msg += f"\n{self=}"
            msg += f"\n{compressed=}"
            msg += f"\n{obj=}"
            msg += f"\nBuffer: {buffer}"
            raise ValueError(msg)
        return obj
