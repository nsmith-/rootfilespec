from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated

from rootfilespec.bootstrap.TKey import TKey
from rootfilespec.bootstrap.TUUID import TUUID
from rootfilespec.bootstrap.util import fDatime_to_datetime
from rootfilespec.dispatch import DICTIONARY
from rootfilespec.structutil import (
    DataFetcher,
    Fmt,
    ReadBuffer,
    ROOTSerializable,
    serializable,
)

"""
TODO: TDirectory for ROOT 3.02.06

Format of a TDirectory record in release 3.02.06. It is never compressed.
       0->0  Modified  = True if directory has been modified                           TDirectory::fModified
       1->1  Writable = True if directory is writable                                  TDirectory::fWriteable
       2->5  DatimeC   = Date and time when directory was created                      TDirectory::fDatimeC
                       | (year-1995)<<26|month<<22|day<<17|hour<<12|minute<<6|second
       6->9  DatimeM   = Date and time when directory was last modified                TDirectory::fDatimeM
                       | (year-1995)<<26|month<<22|day<<17|hour<<12|minute<<6|second
      10->13 NbytesKeys= Number of bytes in the associated KeysList record             TDirectory::fNbyteskeys
      14->17 NbytesName= Number of bytes in TKey+TNamed at creation                    TDirectory::fNbytesName
      18->21 SeekDir   = Byte offset of directory record in file                       TDirectory::fSeekDir
      22->25 SeekParent= Byte offset of parent directory record in file                TDirectory::fSeekParent
      26->29 SeekKeys  = Byte offset of associated KeysList record in file             TDirectory::fSeekKeys
"""


@serializable
class TDirectory_header_v622(ROOTSerializable):
    """Format of a TDirectory record in release 6.22.06. It is never compressed.

    Header information from https://root.cern/doc/master/tdirectory.html

    Attributes:
        fVersion (int): TDirectory class version identifier (+ 1000 if seek is 64 bit)
        fDatimeC (int): Date and time when directory was created
        fDatimeM (int): Date and time when directory was last modified
        fNbytesKeys (int): Number of bytes in the associated KeysList record
        fNbytesName (int): Number of bytes in TKey+TNamed at creation
    """

    fVersion: Annotated[int, Fmt(">h")]
    fDatimeC: Annotated[int, Fmt(">I")]
    fDatimeM: Annotated[int, Fmt(">I")]
    fNbytesKeys: Annotated[int, Fmt(">i")]
    fNbytesName: Annotated[int, Fmt(">i")]

    def version(self) -> int:
        """Version of the TDirectory class"""
        return self.fVersion % 1000

    def is_large(self) -> bool:
        """True if the file is larger than 2GB"""
        return self.fVersion > 1000

    def create_time(self):
        """Date and time when directory was created"""
        return fDatime_to_datetime(self.fDatimeC)

    def modify_time(self):
        """Date and time when directory was last modified"""
        return fDatime_to_datetime(self.fDatimeM)


@serializable
class TDirectory(ROOTSerializable):
    """TDirectory object

    Attributes:
        header (TDirectory_header_v622): TDirectory header information
        fSeekDir (int): Byte offset of directory record in file
        fSeekParent (int): Byte offset of parent directory record in file
        fSeekKeys (int): Byte offset of associated KeysList record in file
        fUUID (TUUID): Universally Unique Identifier
    """

    header: TDirectory_header_v622
    fSeekDir: int
    fSeekParent: int
    fSeekKeys: int
    fUUID: TUUID | None

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        header, buffer = TDirectory_header_v622.read(buffer)
        if header.is_large():
            (fSeekDir, fSeekParent, fSeekKeys), buffer = buffer.unpack(">qqq")
        else:
            (fSeekDir, fSeekParent, fSeekKeys), buffer = buffer.unpack(">iii")
        if header.version() > 1:
            fUUID, buffer = TUUID.read(buffer)
        else:
            fUUID = None
        if not header.is_large():
            # Extra space to allow seeks to become 64 bit without moving this header
            buffer = buffer[12:]
        return (header, fSeekDir, fSeekParent, fSeekKeys, fUUID), buffer

    def get_KeyList(self, fetch_data: DataFetcher):
        buffer = fetch_data(
            self.fSeekKeys, self.header.fNbytesName + self.header.fNbytesKeys
        )
        key, _ = TKey.read(buffer)
        if key.fSeekKey != self.fSeekKeys:
            msg = f"TDirectory.read_keylist: fSeekKey mismatch {key.fSeekKey} != {self.fSeekKeys}"
            raise ValueError(msg)
        if key.fSeekPdir != self.fSeekDir:
            msg = f"TDirectory.read_keylist: fSeekPdir mismatch {key.fSeekPdir} != {self.fSeekDir}"
            raise ValueError(msg)

        def fetch_cached(seek: int, size: int):
            seek -= self.fSeekKeys
            if seek + size <= len(buffer):
                return buffer[seek : seek + size]
            msg = f"TDirectory.read_keylist: fetch_cached: {seek=} {size=} out of range"
            raise ValueError(msg)

        return key.read_object(fetch_cached, objtype=TKeyList)


# TODO: are these different?
DICTIONARY["TDirectory"] = TDirectory
DICTIONARY["TDirectoryFile"] = TDirectory


@serializable
class TKeyList(ROOTSerializable, Mapping[str, TKey]):
    fKeys: list[TKey]
    padding: bytes

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        (nKeys,), buffer = buffer.unpack(">i")
        keys: list[TKey] = []
        while len(keys) < nKeys:
            key, buffer = TKey.read(buffer)
            keys.append(key)
        padding = b""
        if not all(k.is_short() for k in keys):
            # suspicion: there will be 8*nshort trailing bytes
            # corresponding to padding in case seeks need to be 64 bit
            npad = 8 * sum(1 for k in keys if k.is_short())
            padding, buffer = buffer.consume(npad)
        return (keys, padding), buffer

    def __len__(self):
        return len(self.fKeys)

    def __iter__(self):
        return (key.fName.fString.decode("ascii") for key in self.fKeys)

    def __getitem__(self, key: str):
        bkey = key.encode("ascii")
        matches = [k for k in self.fKeys if k.fName.fString == bkey]
        if not len(matches):
            raise KeyError(key)
        return max(matches, key=lambda k: k.header.fCycle)
