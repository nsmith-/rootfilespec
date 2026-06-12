from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import cast

import numpy as np
import pytest
from skhep_testdata import data_path, known_files  # type: ignore[import-not-found]

from rootfilespec.bootstrap import (
    BOOTSTRAP_CONTEXT,
    ROOT3a3aRNTuple,
    ROOTFile,
    TBasket,
    TDirectory,
)
from rootfilespec.bootstrap.compression import decompress
from rootfilespec.bootstrap.streamedobject import Ref, StreamHeader
from rootfilespec.bootstrap.strings import TString
from rootfilespec.bootstrap.TFile import InitialReadLocator
from rootfilespec.bootstrap.TList import TObjArray
from rootfilespec.container import _CArrayReader
from rootfilespec.dispatch import normalize
from rootfilespec.dynamic import build_file_context
from rootfilespec.serializable import (
    BufferContext,
    Locator,
    ReadBuffer,
    ROOTSerializable,
    T_co,
)

TESTABLE_FILES = [f for f in known_files if f.endswith(".root")]


@dataclass(frozen=True)
class DummyLoc:
    offset: int
    size: int

    def read_from(self, buffer: ReadBuffer) -> ROOTSerializable:
        raise NotImplementedError()


def _walk_RNTuple(anchor: ROOT3a3aRNTuple, fetch_data: Callable[[Locator], ReadBuffer]):
    # Collect locators (no I/O yet)
    header_loc = anchor.header_locator
    footer_loc = anchor.footer_locator

    # Fetch header and footer
    header_buffer = fetch_data(header_loc)
    footer_buffer = fetch_data(footer_loc)

    # Deserialize
    _header = header_loc.read_from(header_buffer)
    footer = footer_loc.read_from(footer_buffer)

    # Get pagelist locators (no I/O)
    pagelist_locs = footer.pagelist_locators

    # Fetch and deserialize pagelists
    for pl_loc in pagelist_locs:
        pl_buffer = fetch_data(pl_loc)
        pagelist = pl_loc.read_from(pl_buffer)

        # Get page locators (no I/O)
        page_locs = pagelist.page_locators

        # Fetch and deserialize pages
        for cluster in page_locs:
            for column in cluster:
                for page_loc in column:
                    page_buffer = fetch_data(page_loc)
                    _page = page_loc.read_from(page_buffer)


@dataclass
class _ReadBasket:
    typename: str
    n: int

    @property
    def __name__(self):
        return self.typename

    def read(self, buffer: ReadBuffer):
        items = []
        dyntype = buffer.file_context.type_by_name(self.typename)
        for _ in range(self.n):
            itemheader, _ = StreamHeader.read(buffer)
            item_end = itemheader.fByteCount + 4
            buffer, remaining = buffer[:item_end], buffer[item_end:]
            item, buffer = dyntype.read(buffer)
            if buffer:
                msg = f"Expected buffer to be empty after reading {self.typename}, but got\n{buffer}"
                raise ValueError(msg)
            items.append(item)
            buffer = remaining
        # Now comes some integers?
        members = {"items": items}
        members, buffer = _CArrayReader("offsets", np.dtype(">i4"))(members, buffer)
        return members, buffer


# TODO: use mixins during the class generation to avoid Protocols


class TLeaf:
    fName: TString


class LeafArray:
    objects: tuple[TLeaf | Ref[TLeaf], ...]


class TBranch(ROOTSerializable):
    fName: TString
    fBranches: TObjArray
    fLeaves: LeafArray
    fBasketBytes: np.typing.NDArray[np.int32]
    fBasketSeek: np.typing.NDArray[np.int64]


class TBranchObject(TBranch):
    fClassName: TString


class TBranchElement(TBranchObject):
    fParentName: TString
    fTitle: TString


def _walk_branchlist(
    branchlist: TObjArray,
    fetch_data: Callable[[Locator], ReadBuffer],
    notimplemented_callback: Callable[[bytes, NotImplementedError], None],
    path: bytes = b"",
    indent: int = 0,
):
    for branch in branchlist.objects:
        if type(branch).__name__ not in ("TBranch", "TBranchElement", "TBranchObject"):
            msg = f"Expected TBranch but got {type(branch).__name__}"
            raise TypeError(msg)
        branch = cast(TBranch, branch)
        print(f"{'  ' * indent}Branch: {path + branch.fName.fString!r}")
        _walk_branchlist(
            branch.fBranches,
            fetch_data,
            notimplemented_callback,
            path=path + branch.fName.fString + b".",
            indent=indent + 1,
        )
        if not hasattr(branch, "fClassName"):
            continue  # Simple data type, we trust we can deserialize
        branch = cast(TBranchObject, branch)

        cpptype = branch.fClassName.fString
        if hasattr(branch, "fParentName"):
            branch = cast(TBranchElement, branch)
            if branch.fParentName.fString:
                # The split branch is for a base class
                # apparently the fTitle is the parent branch name + '.' + the type path?
                # e.g. uproot-issue-798.root (xAOD3a3aFileMetaDataAuxInfo_v1)
                cpptype = branch.fTitle.fString.rsplit(b".", 1)[-1]
        typename = normalize(cpptype)
        print(f"{'  ' * indent}  Type: {typename}")

        if len(branch.fLeaves.objects):
            # This is a split branch
            leaves = (
                b"Some" if isinstance(leaf, Ref) else leaf.fName.fString
                for leaf in branch.fLeaves.objects
            )
            print(f"{'  ' * indent}  Leaves: {b','.join(leaves)!r}")
            continue

        for size, seek in zip(branch.fBasketBytes, branch.fBasketSeek, strict=False):
            if size == 0:
                continue
            # TODO: hacky fix for: tests/test_read.py:149: error: Argument 1 has incompatible type "signedinteger[_64Bit]"; expected "int"  [arg-type] (also happened for "signedinteger[_32Bit]")
            # buffer = fetch_data(seek, size)
            buffer = fetch_data(DummyLoc(seek, size))
            basket, buffer = TBasket.read(buffer)
            if len(buffer) == 0:
                if basket.fBuffer is None:
                    msg = "Expected to read a basket with data, but got an empty buffer"
                    raise ValueError(msg)
                buffer = ReadBuffer(
                    basket.fBuffer,
                    basket.header.fKeylen,  # TODO: unsure if this is correct
                    buffer.file_context,
                    BufferContext(abspos=None),
                )
            else:
                # TODO: does the basket always own the buffer?
                msg = "TODO: basket doesn't own its buffer! Existing code might be OK"
                raise ValueError(msg)
            if basket.header.fKeylen != buffer.relpos:
                msg = f"Expected to be at the end of the key after reading the basket, but got {buffer.relpos} != {basket.header.fKeylen}"
                raise ValueError(msg)
            if len(buffer) != basket.header.fObjlen:
                buffer = decompress(buffer, basket.header.fObjlen)
            _ReadBasket(typename, basket.bheader.fNevBuf).read(buffer)


def _walk(
    dir: TDirectory,
    fetch_data: Callable[[Locator], ReadBuffer],
    notimplemented_callback: Callable[[bytes, NotImplementedError], None],
    *,
    depth=0,
    maxdepth=-1,
    path=b"/",
):
    if dir.fSeekKeys == 0:
        # empty directory
        return

    buffer = fetch_data(dir.keylist_locator)
    try:
        keylist_key = dir.keylist_locator.read_from(buffer)
        keylist = keylist_key.read_from(buffer)
    except NotImplementedError as ex:
        notimplemented_callback(path, ex)
        return

    for item in keylist.values():
        itempath = path + item.fName.fString
        buffer = fetch_data(item)
        try:
            obj = item.read_from(buffer)
        except NotImplementedError as ex:
            notimplemented_callback(itempath, ex)
            continue
        if isinstance(obj, TDirectory) and (maxdepth < 0 or depth < maxdepth):
            _walk(
                obj,
                fetch_data,
                notimplemented_callback,
                depth=depth + 1,
                path=itempath + b"/",
            )
        elif isinstance(obj, ROOT3a3aRNTuple):
            _walk_RNTuple(obj, fetch_data)
        elif type(obj).__name__ == "TTree":
            _walk_branchlist(
                obj.fBranches,  # type: ignore[attr-defined]
                fetch_data,
                notimplemented_callback,
                itempath + b"/",
            )


def fetch_cached(buffer: ReadBuffer, loc: Locator[T_co]) -> T_co:
    seek, size = loc.offset, loc.size
    if seek + size <= len(buffer):
        return loc.read_from(buffer[seek : seek + size])
    msg = "Didn't find data in cached buffer"
    raise ValueError(msg)


@pytest.mark.parametrize("filename", TESTABLE_FILES)
def test_read_file(filename: str):
    path = Path(data_path(filename))
    with path.open("rb") as filehandle:

        def fetch_buffer(loc: Locator):
            seek, size = loc.offset, loc.size
            filehandle.seek(seek)
            return ReadBuffer(
                memoryview(filehandle.read(size)),
                0,
                BOOTSTRAP_CONTEXT,
                BufferContext(abspos=seek),
            )

        buffer = fetch_buffer(InitialReadLocator())
        file, _ = ROOTFile.read(buffer)

        fetch_begin = partial(fetch_cached, buffer)
        tfilekey = fetch_begin(file.tfile_locator)
        tfile = fetch_begin(tfilekey)
        rootdir = tfile.rootdir

        # List to collect NotImplementedError messages
        failures: list[str] = []

        def fail_cb(_: bytes, ex: NotImplementedError):
            print(f"NotImplementedError: {ex}")
            failures.append(str(ex))

        buffer = None
        # Read all StreamerInfo (class definitions) from the file
        # two test files have non-null locators but they point beyond the end of
        # the file, so we have to check for that
        if file.streamerinfo_locator:
            buffer = fetch_buffer(file.streamerinfo_locator)

        if not buffer:
            # Try to read all objects anyway
            _walk(rootdir, fetch_buffer, fail_cb)
            if failures:
                return pytest.xfail(reason=",".join(set(failures)))
            return None

        assert file.streamerinfo_locator is not None
        # this buffer should contain the key and the TList of StreamerInfo
        streamerinfokey = file.streamerinfo_locator.read_from(buffer)
        streamerinfo = streamerinfokey.read_from(buffer)

        # Render the class definitions into python code
        try:
            file_context = build_file_context(streamerinfo)
        except NotImplementedError as ex:
            return pytest.xfail(reason=str(ex))

        # Define a new fetcher now that we can interpret the file data
        def fetch_after_streamers(loc: Locator) -> ReadBuffer:
            seek, size = loc.offset, loc.size
            print(f"fetch_data {seek=} {size=}")
            filehandle.seek(seek)
            return ReadBuffer(
                memoryview(filehandle.read(size)),
                0,
                file_context,
                BufferContext(abspos=seek),
            )

        # Read all objects from the file
        try:
            _walk(rootdir, fetch_after_streamers, fail_cb)
            if failures:
                return pytest.xfail(reason=",".join(set(failures)))
        except NotImplementedError as ex:
            return pytest.xfail(reason=str(ex))
        finally:
            file_context.purge_module()
