"""Linked attribute sets in the RNTuple footer (#51)

The footer ends with a list frame of linked attribute set records (spec,
*Footer Envelope*, *Linked Attribute Set Record Frame*). A footer written
before format 1.0.1.0 ends before it, and ROOT reads it only if bytes remain
before the checksum (root-io-spec RNTuple ERRATA 11; RNTupleSerialize.cxx:2015-2017
at 6.40.04).
"""

from pathlib import Path

import pytest
from skhep_testdata import data_path  # type: ignore[import-not-found]

from rootfilespec.bootstrap import BOOTSTRAP_CONTEXT, ROOT3a3aRNTuple
from rootfilespec.bootstrap.TFile import InitialReadLocator
from rootfilespec.reader import Fetcher
from rootfilespec.rntuple.footer import FooterEnvelope, LinkedAttributeSet
from rootfilespec.rntuple.RFrame import ListFrame
from rootfilespec.rntuple.RLocator import StandardLocator, UnknownLocator

DATA = Path(__file__).parent.parent / "reference" / "root-io-spec" / "data" / "rntuple"
FIXTURES = sorted(DATA.glob("*.root"))


def _fetcher(data: bytes) -> Fetcher:
    """An RNTuple needs only the bootstrap classes"""

    def read_at(offset: int, size: int) -> bytes:
        return data[offset : offset + size]

    return Fetcher(read_at, BOOTSTRAP_CONTEXT)


def _anchors(fetch: Fetcher) -> list[ROOT3a3aRNTuple]:
    """The anchors of the RNTuples the top directory lists"""
    file = fetch(InitialReadLocator())
    tfile = fetch.resolve(file.tfile_locator)
    keylist = fetch.resolve(tfile.rootdir.keylist_locator)
    anchors = []
    for key in keylist.values():
        if key.fClassName == b"ROOT::RNTuple":
            anchor = fetch(key)
            assert isinstance(anchor, ROOT3a3aRNTuple)
            anchors.append(anchor)
    return anchors


def _footers(path: Path) -> list[FooterEnvelope]:
    fetch = _fetcher(path.read_bytes())
    return [fetch.rntuple(anchor).footerEnvelope for anchor in _anchors(fetch)]


@pytest.mark.skipif(not FIXTURES, reason="reference/root-io-spec not checked out")
def test_attribute_set_records():
    """rntuple/attributes.root's main footer, against its case.toml: the list
    frame at 3824, and the records of "runs" at 3836 and "flags" at 3872"""
    (footer,) = _footers(DATA / "attributes.root")
    assert footer.attributeSets == ListFrame(
        fSize=85,
        items=[
            LinkedAttributeSet(
                fSize=36,
                fSchemaVersionMajor=1,
                fSchemaVersionMinor=0,
                fAnchorLength=78,
                locator=StandardLocator(size=78, offset=2508),
                fName=b"runs",
            ),
            LinkedAttributeSet(
                fSize=37,
                fSchemaVersionMajor=1,
                fSchemaVersionMinor=0,
                fAnchorLength=78,
                locator=StandardLocator(size=78, offset=3451),
                fName=b"flags",
            ),
        ],
    )
    assert footer._unknown == b""


@pytest.mark.parametrize("path", FIXTURES, ids=lambda path: path.name)
def test_empty_list_is_read(path: Path):
    """Every fixture is of format 1.0.2.0 (ERRATA 1), so every footer has the
    list, empty but for attributes.root's, and nothing is left unread"""
    for footer in _footers(path):
        assert footer.attributeSets is not None
        if path.name != "attributes.root":
            assert footer.attributeSets == ListFrame(fSize=12, items=[])
        assert footer._unknown == b""


@pytest.mark.parametrize(
    "filename",
    [
        "rntviewer-testfile-uncomp-single-rntuple-v1-0-0-0.root",
        "rntviewer-testfile-multiple-rntuples-v1-0-0-0.root",
    ],
)
def test_footer_without_the_list(filename: str):
    """Format 1.0.0.0 has no list: the footer ends at its cluster groups"""
    footers = _footers(Path(data_path(filename)))
    assert footers
    for footer in footers:
        assert footer.attributeSets is None
        assert footer._unknown == b""


def _attribute_sets() -> tuple[Fetcher, list[LinkedAttributeSet]]:
    """attributes.root's fetcher, and the records of its main footer"""
    fetch = _fetcher((DATA / "attributes.root").read_bytes())
    (anchor,) = _anchors(fetch)
    records = fetch.rntuple(anchor).footerEnvelope.attributeSets
    assert records is not None
    return fetch, records.items


@pytest.mark.skipif(not FIXTURES, reason="reference/root-io-spec not checked out")
def test_sets_open_through_their_records():
    """Neither set's anchor is in the key list, which holds only ntpl (case.toml,
    4103). Each record's anchor_locator reads it at the locator's offset, and
    the set opens as an RNTuple named after it"""
    fetch, records = _attribute_sets()
    runs, flags = records
    assert (runs.anchor_locator.offset, runs.anchor_locator.size) == (2508, 78)
    assert (flags.anchor_locator.offset, flags.anchor_locator.size) == (3451, 78)
    runs_anchor = fetch(runs.anchor_locator)
    flags_anchor = fetch(flags.anchor_locator)
    # The anchors' Seek Header, Len Header and Seek Footer the case.toml pins
    assert (
        runs_anchor.fSeekHeader,
        runs_anchor.fLenHeader,
        runs_anchor.fSeekFooter,
    ) == (588, 462, 2302)
    assert (flags_anchor.fSeekHeader, flags_anchor.fSeekFooter) == (1092, 3244)
    for record, anchor in ((runs, runs_anchor), (flags, flags_anchor)):
        rntuple = fetch.rntuple(anchor)
        assert rntuple.headerEnvelope.fName == record.fName
        fields = list(enumerate(rntuple.schemaDescription.fieldDescriptions))
        # Schema 1.0's fields, top-level (their own parents), then the user's
        # under _userData, field 2
        assert [f.fFieldName for i, f in fields if f.fParentFieldID == i] == [
            b"_rangeStart",
            b"_rangeLen",
            b"_userData",
        ]
        assert [f.fFieldName for i, f in fields if f.fParentFieldID == 2 != i] == (
            [b"run", b"weight"] if record is runs else [b"flag"]
        )
        # Restriction 1: an attribute set links no attribute sets of its own
        assert rntuple.footerEnvelope.attributeSets == ListFrame(fSize=12, items=[])


@pytest.mark.skipif(not FIXTURES, reason="reference/root-io-spec not checked out")
def test_ranges():
    """The _rangeStart and _rangeLen pages of each set, at the offsets that the
    case.toml pins them: "flags" holds the empty range (3, 0) first"""
    fetch, records = _attribute_sets()
    ranges = {}
    for record in records:
        rntuple = fetch.rntuple(fetch(record.anchor_locator))
        (cluster,) = rntuple.clusters()
        columns = {}
        for columnRange in cluster.columnRanges[:2]:
            (page,) = columnRange.pages
            locator = page.pageDescription.page_locator
            data = fetch(locator).page
            values = [int.from_bytes(data[i : i + 8], "little") for i in (0, 8)]
            columns[columnRange.column.fieldPath] = (locator.offset, values)
        ranges[record.fName] = columns
    assert ranges == {
        b"runs": {b"_rangeStart": (1768, [0, 3]), b"_rangeLen": (1834, [3, 3])},
        b"flags": {b"_rangeStart": (2812, [3, 1]), b"_rangeLen": (2878, [0, 4])},
    }


def test_anchor_not_in_the_file():
    record = LinkedAttributeSet(
        fSize=0,
        fSchemaVersionMajor=1,
        fSchemaVersionMinor=0,
        fAnchorLength=78,
        locator=UnknownLocator(0x7E, 0, b""),
        fName=b"runs",
    )
    with pytest.raises(NotImplementedError, match="attribute set b'runs'"):
        _ = record.anchor_locator
