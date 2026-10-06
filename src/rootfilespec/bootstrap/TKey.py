from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Generic, TypeVar, overload

from rootfilespec.bootstrap.compression import decompress
from rootfilespec.bootstrap.strings import TString
from rootfilespec.bootstrap.TDatime import TDatime, TDatime_to_datetime
from rootfilespec.dispatch import normalize
from rootfilespec.serializable import (
    Members,
    ReadBuffer,
    ROOTSerializable,
    read_value,
    serializable,
)
from rootfilespec.structutil import Fmt, ROOTString


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

    def write_time(self) -> datetime | None:
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
        fClassName, buffer = ROOTString("TString").read(buffer)
        fName, buffer = ROOTString("TString").read(buffer)
        fTitle, buffer = ROOTString("TString").read(buffer)
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
    def _read_payload(self, buffer: ReadBuffer, objtype: None) -> object: ...

    @overload
    def _read_payload(self, buffer: ReadBuffer, objtype: type[ObjType]) -> ObjType: ...

    def _read_payload(
        self, buffer: ReadBuffer, objtype: type[ObjType] | None
    ) -> object:
        """Read the object from a buffer holding the key and its payload

        The object is read as objtype, or by default as the key's class
        """
        if self.fClassName == b"RBlob":
            # An RBlob key's fObjLen is decorative, and one blob can hold several
            # pages, each with a checksum fObjLen does not count (root-io-spec
            # RNTuple NOTES 1). Reading it as a key payload silently truncates it.
            msg = (
                f"TKey at {self.fSeekKey} is an RBlob: RNTuple bytes are found with "
                "the anchor's envelope locators and the page lists, not through their keys"
            )
            raise ValueError(msg)
        # TODO: should we compare the key in the buffer with ourself?
        buffer = buffer[self.header.fKeylen :]
        # The payload is fNbytes - fKeyLen bytes. Decide from the key, not from
        # how many bytes were fetched: a short fetch (a truncated file) is an
        # error, and a longer one (a caller's cache) holds more than the payload
        stored = self.header.fNbytes - self.header.fKeylen
        if len(buffer) < stored:
            msg = (
                f"TKey at {self.fSeekKey}: expected {stored} payload bytes, "
                f"got {len(buffer)}"
            )
            raise ValueError(msg)
        buffer = buffer[:stored]

        compressed = None
        # Compressed iff fObjLen > fNbytes - fKeyLen (root-io-spec Compression §1);
        # a raw payload longer than fObjLen has slack, which is not part of it
        if self.header.is_compressed():
            buffer = decompress(buffer, self.header.fObjlen)
        else:
            buffer = buffer[: self.header.fObjlen]
        readtype: object
        if objtype is None:
            typename = normalize(self.fClassName)
            readtype = buffer.file_context.type_by_name(typename)
        else:
            typename = objtype.__name__
            readtype = objtype
        # A looked-up type may be an annotated builtin, such as TString (#68)
        obj, buffer = read_value(readtype, buffer)
        # Some types we have to handle trailing bytes
        if typename == "TKeyList":
            # TODO: understand this padding
            # if keys are deleted there is extra space?
            remaining_bytes = self.header.fNbytes - buffer.relpos
            buffer = buffer[remaining_bytes:]
        if buffer:
            msg = f"TKey.read_from: buffer not empty after reading object of type {typename}."
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

    def read_from(self, buffer: ReadBuffer) -> object:
        """The key's object: a ROOTSerializable, or a builtin for a class that
        reads as one, such as ``bytes`` for a ``TString`` (#68, #135)"""
        return self._read_payload(buffer, None)


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
        typename = normalize(key.fClassName)
        objtype = buffer.file_context.type_by_name(typename)
        return cls(key=key, objtype=objtype), buffer  # type: ignore[arg-type]

    def read_from(self, buffer: ReadBuffer) -> ObjType:
        return self.key._read_payload(buffer, self.objtype)
