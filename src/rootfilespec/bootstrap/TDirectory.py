from collections.abc import Mapping
from typing import Annotated, Optional

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
    """

    fVersion: Annotated[int, Fmt(">h")]
    """TDirectory class version identifier"""
    fDatimeC: Annotated[int, Fmt(">I")]
    """Date and time when directory was created"""
    fDatimeM: Annotated[int, Fmt(">I")]
    """Date and time when directory was last modified"""
    fNbytesKeys: Annotated[int, Fmt(">i")]
    """Number of bytes in the associated KeysList record"""
    fNbytesName: Annotated[int, Fmt(">i")]
    """Number of bytes in TKey+TNamed at creation"""

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
    """TDirectory object.
    Binary Spec (the DATA section): https://root.cern.ch/doc/master/tdirectory.html
    """

    header: TDirectory_header_v622
    """TDirectory header information"""
    fSeekDir: int
    """Byte offset of directory record in file"""
    fSeekParent: int
    """Byte offset of parent directory record in file"""
    fSeekKeys: int
    """Byte offset of associated KeysList record in file"""
    fUUID: Optional[TUUID]
    """Universally Unique Identifier"""

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
    """The TKeyList for a TDirectory contains all the (visible) TKeys
    For RNTuples, it will only contain the RNTuple Anchor TKey(s)
    Binary Spec: https://root.cern.ch/doc/master/keyslist.html
    """

    fKeys: list[TKey]
    """List of TKey objects"""
    padding: bytes
    """Extra bytes in the end of the TKeyList record (unknown)"""

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        (nKeys,), buffer = buffer.unpack(">i")
        keys: list[TKey] = []
        while len(keys) < nKeys:
            key, buffer = TKey.read(buffer)
            keys.append(key)
        # TODO: absorb padding bytes
        padding = b""
        return (keys, padding), buffer

    def __len__(self):
        return len(self.fKeys)

    def __iter__(self):
        return (key.fName.fString.decode("ascii") for key in self.fKeys)

    def __getitem__(self, key: str):
        bkey = key.encode("ascii")
        matches = [k for k in self.fKeys if k.fName.fString == bkey]
        if not matches:
            raise KeyError(key)
        return max(matches, key=lambda k: k.header.fCycle)
