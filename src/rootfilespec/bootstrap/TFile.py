from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

from rootfilespec.bootstrap.TDirectory import TDirectory
from rootfilespec.bootstrap.TKey import TKey
from rootfilespec.bootstrap.TList import TList
from rootfilespec.bootstrap.TString import TString
from rootfilespec.bootstrap.TUUID import TUUID
from rootfilespec.structutil import (
    DataFetcher,
    Fmt,
    ReadBuffer,
    ROOTSerializable,
    serializable,
)


@dataclass(order=True)
class VersionInfo(ROOTSerializable):
    major: int
    minor: int
    cycle: int
    large: bool = False  # File is larger than 32bit file limit (2GB)

    @classmethod
    def read(cls, buffer: ReadBuffer):
        (version,), buffer = buffer.unpack(">I")
        return cls(
            major=version // 10_000 % 100,
            minor=version // 100 % 100,
            cycle=version % 100,
            large=version > 1_000_000,
        ), buffer


@serializable
class ROOTFile_header_v302(ROOTSerializable):
    """
    A class representing the header structure for ROOTFile version 3.02.06

    Header information from https://root.cern/doc/master/header.html

    Attributes:
        fBEGIN (int): Byte offset of first data record (64)
        fEND (int): Pointer to first free word at the EOF
        fSeekFree (int): Byte offset of FreeSegments record
        fNbytesFree (int): Number of bytes in FreeSegments record
        nfree (int): Number of free data records
        fNbytesName (int): Number of bytes in TKey+TNamed for ROOTFile at creation
        fUnits (int): Number of bytes for file pointers (4)
        fCompress (int): Zip compression level (i.e. 0-9)
        fSeekInfo (int): Byte offset of StreamerInfo record
        fNbytesInfo (int): Number of bytes in StreamerInfo record
        fUUID (bytes): Unique identifier for the file
    """

    fBEGIN: Annotated[int, Fmt(">i")]
    fEND: Annotated[int, Fmt(">i")]
    fSeekFree: Annotated[int, Fmt(">i")]
    fNbytesFree: Annotated[int, Fmt(">i")]
    nfree: Annotated[int, Fmt(">i")]
    fNbytesName: Annotated[int, Fmt(">i")]
    fUnits: Annotated[int, Fmt(">B")]
    fCompress: Annotated[int, Fmt(">i")]
    fSeekInfo: Annotated[int, Fmt(">i")]
    fNbytesInfo: Annotated[int, Fmt(">i")]
    unused: Annotated[bytes, Fmt("18s")]


@serializable
class ROOTFile_header_v622_small(ROOTSerializable):
    """
    A class representing the header structure for ROOTFile version 6.22.06

    If END, SeekFree, or SeekInfo are located past the 32 bit file limit (> 2000000000)
    then these fields will be 8 instead of 4 bytes and 1000000 is added to the file format version.
    The _large variant of this class is used in that case.

    Attributes:
        fBEGIN (int): Byte offset of first data record (100)
        fEND (int): Pointer to first free word at the EOF
        fSeekFree (int): Byte offset of FreeSegments record
        fNbytesFree (int): Number of bytes in FreeSegments record
        nfree (int): Number of free data records
        fNbytesName (int): Number of bytes in TKey+TNamed for ROOTFile at creation
        fUnits (int): Number of bytes for file pointers (4 or 8)
        fCompress (int): Zip compression level (i.e. 0-9)
        fSeekInfo (int): Byte offset of StreamerInfo record
        fNbytesInfo (int): Number of bytes in StreamerInfo record
    """

    fBEGIN: Annotated[int, Fmt(">I")]
    fEND: Annotated[int, Fmt(">I")]
    fSeekFree: Annotated[int, Fmt(">I")]
    fNbytesFree: Annotated[int, Fmt(">I")]
    nfree: Annotated[int, Fmt(">I")]
    fNbytesName: Annotated[int, Fmt(">I")]
    fUnits: Annotated[int, Fmt(">B")]
    fCompress: Annotated[int, Fmt(">I")]
    fSeekInfo: Annotated[int, Fmt(">I")]
    fNbytesInfo: Annotated[int, Fmt(">I")]
    fUUID: TUUID


@serializable
class ROOTFile_header_v622_large(ROOTSerializable):
    __doc__ = ROOTFile_header_v622_small.__doc__

    fBEGIN: Annotated[int, Fmt(">i")]
    fEND: Annotated[int, Fmt(">q")]
    fSeekFree: Annotated[int, Fmt(">q")]
    fNbytesFree: Annotated[int, Fmt(">i")]
    nfree: Annotated[int, Fmt(">i")]
    fNbytesName: Annotated[int, Fmt(">i")]
    fUnits: Annotated[int, Fmt(">B")]
    fCompress: Annotated[int, Fmt(">i")]
    fSeekInfo: Annotated[int, Fmt(">q")]
    fNbytesInfo: Annotated[int, Fmt(">i")]
    fUUID: TUUID


@serializable
class ROOTFile(ROOTSerializable):
    """A class representing a ROOT file.
    Binary Spec: https://root.cern.ch/doc/master/classTFile.html

    Attributes:
        magic (bytes): The magic number identifying the file as a ROOT file.
        fVersion (VersionInfo): The version information of the ROOT file.
        header (Union[ROOTFile_header_v302, ROOTFile_header_v622_small, ROOTFile_header_v622_large]): The header of the ROOT file.
        UUID (Union[bytes, TUUID]): The UUID of the ROOT file.
        padding (bytes): Padding bytes in the ROOT file.

    Methods:
        read(cls, buffer: ReadBuffer) -> Tuple[ROOTFile, ReadBuffer]:
            Reads a ROOT file from the given buffer and returns a ROOTFile instance and the remaining buffer.

        get_TFile(self, fetch_data: DataFetcher) -> TFile:
            Retrieves the TFile object (root directory) from the file using the provided data fetcher.

        get_StreamerInfo(self, fetch_data: DataFetcher) -> TList:
            Retrieves the StreamerInfo (list of streamers) from the file using the provided data fetcher.
    """

    # Fields for the ROOT file header
    magic: bytes
    fVersion: VersionInfo
    # Field for the actual header. The version determines which header to use.
    header: (
        ROOTFile_header_v302 | ROOTFile_header_v622_small | ROOTFile_header_v622_large
    )
    padding: bytes

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        """Reads and parses a ROOT file from the given buffer.
        Binary Spec: https://root.cern.ch/doc/master/classTFile.html
                     https://root.cern.ch/doc/master/header.html
        """
        (magic,), buffer = buffer.unpack("4s")
        if magic != b"root":
            msg = f"ROOTFile.read: magic is not 'root': {magic!r}"
            raise ValueError(msg)
        fVersion, buffer = VersionInfo.read(buffer)
        if fVersion <= VersionInfo(6, 2, 2):
            header, buffer = ROOTFile_header_v302.read(buffer)
        elif not fVersion.large:
            header, buffer = ROOTFile_header_v622_small.read(buffer)  # type: ignore[assignment]
        else:
            header, buffer = ROOTFile_header_v622_large.read(buffer)  # type: ignore[assignment]
        padding, buffer = buffer.consume(header.fBEGIN - buffer.relpos)
        return (magic, fVersion, header, padding), buffer

    def get_TFile(self, fetch_data: DataFetcher):
        """Get the TFile object (root directory) from the file."""
        buffer = fetch_data(self.header.fBEGIN, self.header.fNbytesName)
        key, buffer = TKey.read(buffer)
        if key.fSeekKey != self.header.fBEGIN:
            msg = f"ROOTFile.read_rootkey: key.fSeekKey != self.header.fBEGIN: {key.fSeekKey} != {self.header.fBEGIN}"
            raise ValueError(msg)
        if key.fSeekPdir != 0:
            msg = f"ROOTFile.read_rootkey: key.fSeekPdir != 0: {key.fSeekPdir} != 0"
            raise ValueError(msg)
        return key.read_object(fetch_data, objtype=TFile)

    def get_StreamerInfo(self, fetch_data: DataFetcher):
        buffer = fetch_data(self.header.fSeekInfo, self.header.fNbytesInfo)
        key, _ = TKey.read(buffer)
        if key.fSeekKey != self.header.fSeekInfo:
            msg = f"ROOTFile.get_StreamerInfo: fSeekKey != fSeekInfo: {key.fSeekKey} != {self.header.fSeekInfo}"
            raise ValueError(msg)
        if key.header.fNbytes != self.header.fNbytesInfo:
            msg = f"ROOTFile.get_StreamerInfo: fNbytes != fNbytesInfo: {key.header.fNbytes} != {self.header.fNbytesInfo}"
            raise ValueError(msg)

        def fetch_cached(seek: int, size: int):
            seek -= self.header.fSeekInfo
            return buffer[seek : seek + size]

        return key.read_object(fetch_cached, objtype=TList)


@serializable
class TFile(ROOTSerializable):
    """The TFile object is a TDirectory with an extra name and title field (the first or "root" TDirectory):
        Binary Spec (the DATA section): https://root.cern.ch/doc/master/tfile.html

    TDirectory otherwise has its name and title in its owning TKey object (see TDirectory class).
    """

    # Header fields for the root TDirectory
    fName: TString
    fTitle: TString
    # Field for the actual root TDirectory (formatted like a normal TDirectory)
    rootdir: TDirectory

    def get_KeyList(self, fetch_data):
        return self.rootdir.get_KeyList(fetch_data)
