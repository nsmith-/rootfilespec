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
from rootfilespec.bootstrap.streamedobject import Ref, StreamHeader
from rootfilespec.bootstrap.strings import TString
from rootfilespec.bootstrap.TList import TObjArray
from rootfilespec.container import _CArrayReader
from rootfilespec.dispatch import normalize
from rootfilespec.dynamic import build_file_context
from rootfilespec.serializable import (
    BufferContext,
    DataFetcher,
    ReadBuffer,
    ROOTSerializable,
)

TESTABLE_FILES = [f for f in known_files if f.endswith(".root")]


def _walk_RNTuple(anchor: ROOT3a3aRNTuple, fetch_data: DataFetcher):
    footer = anchor.get_footer(fetch_data)
    pagelists = footer.get_pagelists(fetch_data)
    for page_locations in pagelists:
        page_locations.get_pages(fetch_data)


def _dummy_fetch(buffer: ReadBuffer, _seek: int, _size: int) -> ReadBuffer:
    """A dummy fetch function that does nothing."""
    return buffer


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
    fetch_data: DataFetcher,
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
            buffer = fetch_data(int(seek), int(size))
            basket, _ = TBasket.read(buffer)
            # TODO: this is a bit of a hack to avoid writing the same decompression code as in TKey.read_object
            basket.read_object(
                partial(_dummy_fetch, buffer),
                _ReadBasket(typename, basket.bheader.fNevBuf),
            )


def _walk(
    dir: TDirectory,
    fetch_data: DataFetcher,
    notimplemented_callback: Callable[[bytes, NotImplementedError], None],
    *,
    depth=0,
    maxdepth=-1,
    path=b"/",
):
    if dir.fSeekKeys == 0:
        # empty directory
        return
    keylist = dir.get_KeyList(fetch_data)
    for item in keylist.values():
        itempath = path + item.fName.fString
        try:
            obj = item.read_object(fetch_data)
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
                obj.fBranches, fetch_data, notimplemented_callback, itempath + b"/"
            )


@pytest.mark.parametrize("filename", TESTABLE_FILES)
def test_read_file(filename: str):
    initial_read_size = 512
    path = Path(data_path(filename))
    with path.open("rb") as filehandle:

        def fetch_data(seek: int, size: int):
            filehandle.seek(seek)
            return ReadBuffer(
                memoryview(filehandle.read(size)),
                0,
                BOOTSTRAP_CONTEXT,
                BufferContext(abspos=seek),
            )

        buffer = fetch_data(0, initial_read_size)
        file, _ = ROOTFile.read(buffer)

        # Read root directory object, which should be contained in the initial buffer
        def fetch_cached(seek: int, size: int):
            if seek + size <= len(buffer):
                return buffer[seek : seek + size]
            msg = "Didn't find data in initial read buffer"
            raise ValueError(msg)

        rootdir = file.get_TFile(fetch_cached).rootdir

        # List to collect NotImplementedError messages
        failures: list[str] = []

        def fail_cb(_: bytes, ex: NotImplementedError):
            print(f"NotImplementedError: {ex}")
            failures.append(str(ex))

        # Read all StreamerInfo (class definitions) from the file
        streamerinfo = file.get_StreamerInfo(fetch_data)
        if not streamerinfo:
            # Try to read all objects anyway
            _walk(rootdir, fetch_data, fail_cb)
            if failures:
                return pytest.xfail(reason=",".join(set(failures)))
            return None

        # Render the class definitions into python code
        try:
            file_context = build_file_context(streamerinfo)
        except NotImplementedError as ex:
            return pytest.xfail(reason=str(ex))

        # Define a new fetcher now that we can interpret the file data
        def fetch_after_streamers(seek: int, size: int):
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
