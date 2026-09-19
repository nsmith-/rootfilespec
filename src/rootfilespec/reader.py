"""A minimal synchronous reader on top of the Locator protocol

Objects in this library describe where data is, and callers decide when and how
to fetch it (see docs/design.md). The classes here are the simplest such
caller: given a function returning the bytes at a position in the file, they
fetch what a locator points to, one read at a time. Callers that batch or
await their reads use the locators directly instead.
"""

import os
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import Any

from rootfilespec.bootstrap import BOOTSTRAP_CONTEXT
from rootfilespec.bootstrap.TDirectory import TDirectory, TKeyList
from rootfilespec.bootstrap.TFile import InitialReadLocator, ROOTFile, TFile
from rootfilespec.bootstrap.TKey import ObjType, TypedTKey
from rootfilespec.bootstrap.TList import TList
from rootfilespec.bootstrap.TStreamerInfo import TStreamerInfo
from rootfilespec.dynamic import build_file_context
from rootfilespec.serializable import (
    BufferContext,
    FileContext,
    Locator,
    ReadBuffer,
    T_co,
)

ReadAt = Callable[[int, int], bytes | memoryview]
"""Returns the ``size`` bytes at ``offset`` in the file (fewer at the end of it)"""


@dataclass(frozen=True)
class Fetcher:
    """Fetches and deserializes what a locator points to"""

    read_at: ReadAt
    context: FileContext
    """The types available to interpret the data with"""

    def buffer_at(self, offset: int, size: int) -> ReadBuffer:
        offset, size = int(offset), int(size)
        return ReadBuffer(
            memoryview(self.read_at(offset, size)),
            0,
            self.context,
            BufferContext(abspos=offset),
        )

    def buffer(self, loc: Locator[Any]) -> ReadBuffer:
        return self.buffer_at(loc.offset, loc.size)

    def __call__(self, loc: Locator[T_co]) -> T_co:
        return loc.read_from(self.buffer(loc))

    def resolve(self, loc: Locator[TypedTKey[ObjType]]) -> ObjType:
        """Fetch the object of the key that the locator points to

        The locators of a record found by position (the TFile, the StreamerInfo
        or the key list of a directory) read the key at the front of the record,
        which in turn locates the record. One read serves both if it covers it.
        """
        buffer = self.buffer(loc)
        key = loc.read_from(buffer)
        if key.offset == loc.offset and key.size <= len(buffer):
            return key.read_from(buffer[: key.size])
        return self(key)


@dataclass
class FileReader:
    """The self-describing part of a ROOT file, and a fetcher for the rest of it

    Use as a context manager, or call close(), to release what open() acquired.
    """

    file: ROOTFile
    tfile: TFile
    streamerinfo: TList | None
    """The StreamerInfo record, if the file has one"""
    fetch: Fetcher
    """Interprets data with the types of the StreamerInfo record, if any"""
    _on_close: list[Callable[[], None]] = field(default_factory=list, repr=False)

    @classmethod
    def open(cls, read_at: ReadAt) -> "FileReader":
        initial_loc = InitialReadLocator()
        initial = bytes(read_at(initial_loc.offset, initial_loc.size))

        def read_cached(offset: int, size: int) -> bytes | memoryview:
            if offset + size <= len(initial):
                return initial[offset : offset + size]
            return read_at(offset, size)

        fetch = Fetcher(read_cached, BOOTSTRAP_CONTEXT)
        file = fetch(initial_loc)
        tfile = fetch.resolve(file.tfile_locator)
        streamerinfo = None
        on_close: list[Callable[[], None]] = []
        # Some files have a locator that points beyond the end of the file
        if file.streamerinfo_locator and fetch.buffer(file.streamerinfo_locator):
            streamerinfo = fetch.resolve(file.streamerinfo_locator)
            context = build_file_context(streamerinfo)
            on_close.append(context.purge_module)
            fetch = Fetcher(read_cached, context)
        return cls(file, tfile, streamerinfo, fetch, on_close)

    @property
    def rootdir(self) -> TDirectory:
        return self.tfile.rootdir

    def keylist(self, directory: TDirectory | None = None) -> TKeyList:
        """The keys of a directory (by default, of the top directory)"""
        directory = self.rootdir if directory is None else directory
        if directory.fSeekKeys == 0:
            return TKeyList(fKeys=[], padding=b"")
        return self.fetch.resolve(directory.keylist_locator)

    def streamerinfos(self) -> dict[bytes, TStreamerInfo]:
        """The TStreamerInfo of each class in the StreamerInfo record, by class name"""
        items = self.streamerinfo.items if self.streamerinfo else []
        return {
            item.fName.fString: item
            for item in items
            if isinstance(item, TStreamerInfo)
        }

    def close(self) -> None:
        while self._on_close:
            self._on_close.pop()()

    def __enter__(self) -> "FileReader":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


def open_path(path: str | os.PathLike[str]) -> FileReader:
    """Open a local file. The reader owns the file handle until it is closed."""
    filehandle = Path(path).open("rb")  # noqa: SIM115 (closed by the reader)

    def read_at(offset: int, size: int) -> bytes:
        filehandle.seek(offset)
        return filehandle.read(size)

    try:
        reader = FileReader.open(read_at)
    except BaseException:
        filehandle.close()
        raise
    reader._on_close.append(filehandle.close)
    return reader


__all__ = ["Fetcher", "FileReader", "ReadAt", "open_path"]
