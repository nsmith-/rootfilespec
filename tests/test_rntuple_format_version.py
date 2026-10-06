"""The RNTuple format rootfilespec reads: 1.0.2.x, as ROOT 6.40.04 writes it (#121)

ROOT 6.40.04 ships the RNTuple specification v1.0.2.1, but its writer stamps
patch 0 into the anchor, so every RNTuple it writes says 1.0.2.0 (root-io-spec
RNTuple ERRATA 1). root-io-spec's RNTuple fixtures are all written by it, the
attribute sets of rntuple/attributes.root included.
"""

from pathlib import Path

import pytest

from rootfilespec.bootstrap import ROOT3a3aRNTuple
from rootfilespec.reader import FileReader, open_path
from rootfilespec.rntuple.RNTuple import RNTuple

DATA = Path(__file__).parent.parent / "reference" / "root-io-spec" / "data" / "rntuple"
pytestmark = pytest.mark.skipif(
    not DATA.exists(), reason="reference/root-io-spec not checked out"
)


def _rntuples(reader: FileReader) -> list[tuple[ROOT3a3aRNTuple, RNTuple]]:
    """Every RNTuple of a file, with its anchor: those its key list holds, and the
    attribute sets their footers link, whose anchors no key list holds"""
    out = []
    for key in reader.keylist().values():
        if key.fClassName != b"ROOT::RNTuple":
            continue
        anchor = reader.fetch(key)
        assert isinstance(anchor, ROOT3a3aRNTuple)
        rntuple = reader.fetch.rntuple(anchor)
        out.append((anchor, rntuple))
        for record in rntuple.footerEnvelope.attributeSets or []:
            set_anchor = reader.fetch(record.anchor_locator)
            assert isinstance(set_anchor, ROOT3a3aRNTuple)
            out.append((set_anchor, reader.fetch.attribute_set(record)))
    return out


@pytest.mark.parametrize("name", sorted(p.name for p in DATA.glob("*.root")))
def test_fixture_is_format_1_0_2_0_and_reads(name: str):
    """Each RNTuple's anchor says 1.0.2.0, and the RNTuple reads to its pages"""
    with open_path(DATA / name) as reader:
        rntuples = _rntuples(reader)
        assert rntuples
        for anchor, rntuple in rntuples:
            version = (
                anchor.fVersionEpoch,
                anchor.fVersionMajor,
                anchor.fVersionMinor,
                anchor.fVersionPatch,
            )
            assert version == (1, 0, 2, 0)
            assert rntuple.featureFlags.flags == 0
            for cluster in rntuple.clusters():
                for column_range in cluster.columnRanges:
                    for page in column_range.pages:
                        reader.fetch(page.pageDescription.page_locator)


def test_attribute_sets_are_included():
    """rntuple/attributes.root: the main RNTuple and its two attribute sets"""
    with open_path(DATA / "attributes.root") as reader:
        names = [rntuple.headerEnvelope.fName for _, rntuple in _rntuples(reader)]
    assert sorted(names) == [b"flags", b"ntpl", b"runs"]


def test_version_words_are_the_pinned_bytes():
    """rntuple/anchor.root's case.toml pins the four version words, big-endian
    u16s at 1058, 1060, 1062 and 1064: 1, 0, 2, 0"""
    raw = (DATA / "anchor.root").read_bytes()
    assert raw[1058:1066] == bytes([0, 1, 0, 0, 0, 2, 0, 0])
    with open_path(DATA / "anchor.root") as reader:
        (key,) = [
            k for k in reader.keylist().values() if k.fClassName == b"ROOT::RNTuple"
        ]
        anchor = reader.fetch(key)
    assert isinstance(anchor, ROOT3a3aRNTuple)
    assert (anchor.fVersionMinor, anchor.fVersionPatch) == (2, 0)
