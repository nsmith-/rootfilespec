from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import numpy as np
import pytest
from skhep_testdata import data_path, known_files  # type: ignore[import-not-found]

from rootfilespec.bootstrap import (
    ROOT3a3aRNTuple,
    TBasket,
    TDirectory,
)
from rootfilespec.bootstrap.compression import decompress
from rootfilespec.bootstrap.streamedobject import Ref, StreamHeader
from rootfilespec.bootstrap.strings import TString
from rootfilespec.bootstrap.TList import TObjArray
from rootfilespec.container import _CArrayReader
from rootfilespec.dispatch import normalize
from rootfilespec.reader import Fetcher, FileReader, open_path
from rootfilespec.serializable import (
    BufferContext,
    ReadBuffer,
    ROOTSerializable,
)

TESTABLE_FILES = [f for f in known_files if f.endswith(".root")]


def _walk_RNTuple(anchor: ROOT3a3aRNTuple, fetch: Fetcher):
    _header = fetch(anchor.header_locator)
    footer = fetch(anchor.footer_locator)
    for pagelist_loc in footer.pagelist_locators:
        pagelist = fetch(pagelist_loc)
        for cluster in pagelist.page_locators:
            for column in cluster:
                for page_loc in column:
                    _page = fetch(page_loc)


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
    fetch: Fetcher,
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
            fetch,
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
            buffer = fetch.buffer_at(int(seek), int(size))
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
    reader: FileReader,
    dir: TDirectory,
    notimplemented_callback: Callable[[bytes, NotImplementedError], None],
    *,
    depth=0,
    maxdepth=-1,
    path=b"/",
):
    try:
        keylist = reader.keylist(dir)
    except NotImplementedError as ex:
        notimplemented_callback(path, ex)
        return

    for item in keylist.values():
        itempath = path + item.fName.fString
        try:
            obj = reader.fetch(item)
        except NotImplementedError as ex:
            notimplemented_callback(itempath, ex)
            continue
        if isinstance(obj, TDirectory) and (maxdepth < 0 or depth < maxdepth):
            _walk(
                reader,
                obj,
                notimplemented_callback,
                depth=depth + 1,
                path=itempath + b"/",
            )
        elif isinstance(obj, ROOT3a3aRNTuple):
            _walk_RNTuple(obj, reader.fetch)
        elif type(obj).__name__ == "TTree":
            _walk_branchlist(
                obj.fBranches,  # type: ignore[attr-defined]
                reader.fetch,
                notimplemented_callback,
                itempath + b"/",
            )


def read_file(path: Path) -> None:
    """Read everything we know how to read from the ROOT file at ``path``

    Raises NotImplementedError if any part of the file could not be read because
    of a known-unimplemented feature; any other exception is a genuine failure.
    """
    # List to collect NotImplementedError messages
    failures: list[str] = []

    def fail_cb(_: bytes, ex: NotImplementedError):
        print(f"NotImplementedError: {ex}")
        failures.append(str(ex))

    # Opening renders the StreamerInfo (class definitions) into python code
    with open_path(path) as reader:
        _walk(reader, reader.rootdir, fail_cb)
    if failures:
        raise NotImplementedError(",".join(set(failures)))


@pytest.mark.parametrize("filename", TESTABLE_FILES)
def test_read_file(filename: str):
    try:
        read_file(Path(data_path(filename)))
    except NotImplementedError as ex:
        pytest.xfail(reason=str(ex))
