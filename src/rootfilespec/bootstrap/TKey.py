from dataclasses import dataclass
from typing import Annotated, Generic, TypeVar, overload

from rootfilespec.bootstrap.compression import decompress
from rootfilespec.bootstrap.strings import TString
from rootfilespec.bootstrap.TDatime import TDatime, TDatime_to_datetime
from rootfilespec.dispatch import normalize
from rootfilespec.serializable import (
    DataFetcher,
    Members,
    ReadBuffer,
    ROOTSerializable,
    serializable,
)
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
        """Return if the key is short (i.e. the seeks are 32 bit)

        A key is large when fVersion > 1000 (TKey.cxx:660, :1269; root-io-spec
        Record §2). RNTuple's own mini-file parser uses >= 1000, but a reader
        should follow TKey (root-io-spec Record erratum 9).
        """
        return self.fVersion <= 1000

    def is_compressed(self) -> bool:
        """Return if the key's payload is compressed

        It is compressed if fObjLen > fNbytes - fKeyLen, the test at all of
        ROOT's read sites (root-io-spec Compression §1). A raw payload may be
        longer than fObjLen; the extra bytes are slack.
        """
        return self.fObjlen > self.fNbytes - self.fKeylen

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
        if header.is_short():
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
        objtype: type[ObjType] | None = None,
    ) -> ObjType | ROOTSerializable:
        if self.fClassName.fString == b"RBlob":
            # An RBlob key's fObjLen is decorative, and one blob can hold several
            # pages, each with a checksum fObjLen does not count (root-io-spec
            # RNTuple NOTES 1). Reading it as a key payload silently truncates it.
            msg = (
                f"TKey at {self.fSeekKey} is an RBlob: RNTuple bytes are found with "
                "the anchor's envelope locators and the page lists, not through their keys"
            )
            raise ValueError(msg)
        buffer = fetch_data(self.fSeekKey, self.header.fNbytes)
        # TODO: should we compare the key in the buffer with ourself?
        buffer = buffer[self.header.fKeylen :]

        compressed = None
        # Compressed iff fObjLen > fNbytes - fKeyLen (root-io-spec Compression §1);
        # a raw payload longer than fObjLen has slack, which is not part of it
        if self.header.fObjlen > len(buffer):
            buffer = decompress(buffer, self.header.fObjlen)
        else:
            buffer = buffer[: self.header.fObjlen]
        if objtype is not None:
            typename = objtype.__name__
            obj, buffer = objtype.read(buffer)
        else:
            typename = normalize(self.fClassName.fString)
            dyntype = buffer.file_context.type_by_name(typename)
            obj, buffer = dyntype.read(buffer)  # type: ignore[assignment]
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
        return obj

    @property
    def offset(self) -> int:
        return self.fSeekKey

    @property
    def size(self) -> int:
        return self.header.fNbytes

    def read_from(self, buffer: ReadBuffer) -> ROOTSerializable:
        return self.read_object(lambda _seek, _size: buffer)


@dataclass(frozen=True)
class TypedTKey(Generic[ObjType], ROOTSerializable):
    """A TKey with a specified object type."""

    key: TKey
    objtype: type[ObjType]

    @property
    def offset(self) -> int:
        return self.key.offset

    @property
    def size(self) -> int:
        return self.key.size

    @classmethod
    def read(cls, buffer: ReadBuffer):
        key, buffer = TKey.read(buffer)
        typename = normalize(key.fClassName.fString)
        objtype = buffer.file_context.type_by_name(typename)
        return cls(key=key, objtype=objtype), buffer  # type: ignore[arg-type]

    def read_from(self, buffer: ReadBuffer) -> ObjType:
        return self.key.read_object(lambda _seek, _size: buffer, objtype=self.objtype)
