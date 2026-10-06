"""Building an RNTuple from its envelopes, and the ways of fetching them

RNTuple.from_envelopes does no I/O (#114). Fetcher.rntuple fetches the
envelopes one read at a time, and needs only the bootstrap classes, so it also
works on a fetcher on BOOTSTRAP_CONTEXT: the path for files whose StreamerInfo
can't be turned into classes, such as the CMS MiniAOD files (#41).
"""

import dataclasses
from pathlib import Path

import pytest

from rootfilespec.bootstrap import BOOTSTRAP_CONTEXT, ROOT3a3aRNTuple
from rootfilespec.bootstrap.TDirectory import TKeyList
from rootfilespec.bootstrap.TFile import InitialReadLocator
from rootfilespec.reader import Fetcher, FileReader, open_path
from rootfilespec.rntuple.RNTuple import RNTuple

DATA = Path(__file__).parent.parent / "reference" / "root-io-spec" / "data" / "rntuple"
FIXTURES = sorted(DATA.glob("*.root")) if DATA.exists() else []

pytestmark = pytest.mark.skipif(
    not DATA.exists(), reason="root-io-spec not checked out"
)


def _anchors(fetch: Fetcher, keylist: TKeyList) -> dict[bytes, ROOT3a3aRNTuple]:
    anchors = {}
    for key in keylist.values():
        if key.fClassName == b"ROOT::RNTuple":
            anchor = fetch(key)
            assert isinstance(anchor, ROOT3a3aRNTuple)
            anchors[key.fName] = anchor
    return anchors


def _reader_rntuples(reader: FileReader) -> dict[bytes, RNTuple]:
    anchors = _anchors(reader.fetch, reader.keylist())
    return {name: reader.fetch.rntuple(anchor) for name, anchor in anchors.items()}


def _anchor(path: Path) -> tuple[FileReader, ROOT3a3aRNTuple]:
    reader = open_path(path)
    (anchor,) = _anchors(reader.fetch, reader.keylist()).values()
    return reader, anchor


@pytest.mark.parametrize("path", FIXTURES, ids=lambda p: p.name)
def test_by_hand_equals_fetcher(path: Path):
    """The envelopes fetched through their locators and given to from_envelopes
    make the RNTuple that Fetcher.rntuple makes"""
    with open_path(path) as reader:
        anchors = _anchors(reader.fetch, reader.keylist())
        assert anchors
        for anchor in anchors.values():
            header = reader.fetch(anchor.header_locator)
            footer = reader.fetch(anchor.footer_locator)
            pagelists = [reader.fetch(loc) for loc in footer.pagelist_locators]
            by_hand = RNTuple.from_envelopes(header, footer, pagelists)
            assert by_hand == reader.fetch.rntuple(anchor)


@pytest.mark.parametrize("path", FIXTURES, ids=lambda p: p.name)
def test_bootstrap_only_equals_reader(path: Path):
    """A fetcher on BOOTSTRAP_CONTEXT, with no classes from the StreamerInfo,
    reads every RNTuple as a FileReader does"""
    with path.open("rb") as filehandle:

        def read_at(offset: int, size: int) -> bytes:
            filehandle.seek(offset)
            return filehandle.read(size)

        fetch = Fetcher(read_at, BOOTSTRAP_CONTEXT)
        file = fetch(InitialReadLocator())
        tfile = fetch.resolve(file.tfile_locator)
        keylist = fetch.resolve(tfile.rootdir.keylist_locator)
        bootstrap = {
            name: fetch.rntuple(anchor)
            for name, anchor in _anchors(fetch, keylist).items()
        }
    with open_path(path) as reader:
        assert bootstrap == _reader_rntuples(reader)
    assert bootstrap


def test_footer_for_another_header_raises():
    reader, anchor = _anchor(DATA / "anchor.root")
    with reader:
        header = reader.fetch(anchor.header_locator)
        footer = reader.fetch(anchor.footer_locator)
        pagelists = [reader.fetch(loc) for loc in footer.pagelist_locators]
    other = dataclasses.replace(footer, headerChecksum=footer.headerChecksum ^ 1)
    with pytest.raises(ValueError, match="^Header checksum mismatch"):
        RNTuple.from_envelopes(header, other, pagelists)


def test_pagelist_for_another_header_raises():
    reader, anchor = _anchor(DATA / "anchor.root")
    with reader:
        header = reader.fetch(anchor.header_locator)
        footer = reader.fetch(anchor.footer_locator)
        pagelists = [reader.fetch(loc) for loc in footer.pagelist_locators]
    assert pagelists
    other = dataclasses.replace(
        pagelists[-1], headerChecksum=pagelists[-1].headerChecksum ^ 1
    )
    with pytest.raises(ValueError, match="PageListEnvelope header checksum mismatch"):
        RNTuple.from_envelopes(header, footer, [*pagelists[:-1], other])
