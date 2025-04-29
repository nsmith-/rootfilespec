from typing import Annotated, Optional, TypeVar, Union, overload

from rootfilespec.bootstrap.compression import RCompressed
from rootfilespec.bootstrap.strings import TString
from rootfilespec.bootstrap.TDatime import TDatime, TDatime_to_datetime
from rootfilespec.buffer import DataFetcher, ReadBuffer
from rootfilespec.dispatch import DICTIONARY, normalize
from rootfilespec.serializable import Members, ROOTSerializable, serializable
from rootfilespec.structutil import Fmt


@serializable
class TKey_header(ROOTSerializable):
    """TKey header information"""

    fNbytes: Annotated[int, Fmt(">i")]
    """Number of bytes in compressed record (Tkey+data)"""
    fVersion: Annotated[int, Fmt(">h")]
    """TKey class version identifier"""
    fObjlen: Annotated[int, Fmt(">i")]
    """Number of bytes of uncompressed data"""
    fDatime: TDatime
    """Date and time when record was written to file"""
    fKeylen: Annotated[int, Fmt(">h")]
    """Number of bytes in key structure (TKey)"""
    fCycle: Annotated[int, Fmt(">h")]
    """Cycle of key"""

    def write_time(self):
        """Date and time when record was written to file"""
        return TDatime_to_datetime(self.fDatime)

    def is_short(self) -> bool:
        """Return if the key is short (i.e. the seeks are 32 bit)"""
        return self.fVersion < 1000

    def is_compressed(self) -> bool:
        """Return if the key is compressed"""
        return self.fNbytes != self.fObjlen + self.fKeylen

    def is_embedded(self) -> bool:
        """Return if the key's payload is embedded"""
        return self.fNbytes <= self.fKeylen


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
    def update_members(
        cls, members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        header, buffer = TKey_header.read(buffer)
        if header.fVersion < 1000:
            (fSeekKey, fSeekPdir), buffer = buffer.unpack(">ii")
        else:
            (fSeekKey, fSeekPdir), buffer = buffer.unpack(">qq")
        fClassName, buffer = TString.read(buffer)
        fName, buffer = TString.read(buffer)
        fTitle, buffer = TString.read(buffer)
        if header.fVersion % 1000 not in (2, 4):
            msg = f"TKey.read_members: unexpected version {header.fVersion}"
            raise ValueError(msg)
        members["header"] = header
        members["fSeekKey"] = fSeekKey
        members["fSeekPdir"] = fSeekPdir
        members["fClassName"] = fClassName
        members["fName"] = fName
        members["fTitle"] = fTitle
        return members, buffer

    @overload
    def read_object(self, fetch_data: DataFetcher) -> ROOTSerializable: ...

    @overload
    def read_object(
        self, fetch_data: DataFetcher, objtype: type[ObjType]
    ) -> ObjType: ...

    def read_object(
        self,
        fetch_data: DataFetcher,
        objtype: Optional[type[ObjType]] = None,
    ) -> Union[ObjType, ROOTSerializable]:
        buffer = fetch_data(self.fSeekKey, self.header.fNbytes)
        # TODO: should we compare the key in the buffer with ourself?
        buffer = buffer[self.header.fKeylen :]

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
            obj, buffer = objtype.read(buffer)
        else:
            typename = normalize(self.fClassName.fString)
            dyntype = DICTIONARY.get(typename)
            if dyntype is None:
                msg = f"TKey.read_object: unknown type {typename}"
                raise NotImplementedError(msg)
            obj, buffer = dyntype.read(buffer)
        # Some types we have to handle trailing bytes
        if typename == "TKeyList":
            # TODO: understand this padding
            # if keys are deleted there is extra space?
            remaining_bytes = self.header.fNbytes - buffer.relpos
            buffer = buffer[remaining_bytes:]
        elif typename == "ROOT3a3aRNTuple":
            # A checksum is added to the end of the buffer
            # TODO: implement checksum verification
            buffer = buffer[8:]
        if buffer:
            msg = f"TKey.read_object: buffer not empty after reading object of type {typename}."
            msg += f"\n{self=}"
            msg += f"\n{compressed=}"
            msg += f"\n{obj=}"
            msg += f"\nBuffer: {buffer}"
            raise ValueError(msg)
        # mypy doesn't understand that we have obj: ObjType | ROOTSerializable here
        return obj  # type: ignore[no-any-return]
