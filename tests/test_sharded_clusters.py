import struct
from pathlib import Path

import pytest
import xxhash  # type: ignore[import-not-found]

from rootfilespec.bootstrap import ROOT3a3aRNTuple
from rootfilespec.reader import FileReader
from rootfilespec.rntuple.RNTuple import RNTuple

DATA = Path(__file__).parent.parent / "reference" / "root-io-spec" / "data" / "rntuple"
pytestmark = pytest.mark.skipif(
    not DATA.exists(), reason="reference/root-io-spec not checked out"
)

# anchor.root is uncompressed. Its one page list envelope is at 632, 164 bytes
# long (the footer's cluster group); after the envelope's type and length, the
# header checksum and the cluster summary list frame's size and item count, the
# one cluster summary's record frame size is at 660, fFirstEntryNumber at 668
# and fNEntriesAndFeatureFlag at 676, whose top byte (683) holds the flags.
PAGELIST, PAGELIST_LENGTH = 632, 164
N_ENTRIES_AND_FLAGS = 676


def _read(flags: int) -> RNTuple:
    """anchor.root with the cluster summary's flags set to ``flags``, and the
    page list envelope's checksum recomputed so that only the flags differ"""
    data = bytearray((DATA / "anchor.root").read_bytes())
    assert struct.unpack_from("<Q", data, N_ENTRIES_AND_FLAGS) == (3,)
    data[N_ENTRIES_AND_FLAGS + 7] = flags
    end = PAGELIST + PAGELIST_LENGTH - 8
    struct.pack_into("<Q", data, end, xxhash.xxh3_64_intdigest(data[PAGELIST:end]))

    reader = FileReader.open(lambda offset, size: bytes(data[offset : offset + size]))
    (key,) = [k for k in reader.keylist().values() if k.fClassName == b"ROOT::RNTuple"]
    anchor = reader.fetch(key)
    assert isinstance(anchor, ROOT3a3aRNTuple)
    return reader.fetch.rntuple(anchor)


def test_unchanged():
    """The patching itself changes nothing: anchor.root reads as it is"""
    (summary,) = _read(0x00).pagelistEnvelopes[0].clusterSummaries
    assert summary.fNEntriesAndFeatureFlag == 3


@pytest.mark.parametrize("flags", [0x01, 0x03, 0xFF])
def test_sharded_cluster_flag_raises(flags: int):
    """Issue #143: readers abort when a cluster summary has flag 0x01 (spec,
    Cluster Summary Record Frame; RNTupleSerialize.cxx:1239-1240)"""
    with pytest.raises(NotImplementedError, match=r"sharded-cluster flag \(0x01\)"):
        _read(flags)


def test_other_flags_are_kept_and_ignored():
    """Other flags are ignored, and the stored value is kept as it is"""
    rntuple = _read(0x02)
    (summary,) = rntuple.pagelistEnvelopes[0].clusterSummaries
    assert summary.fNEntriesAndFeatureFlag == (0x02 << 56) | 3
    assert summary.fFeatureFlag == 0x02
    assert summary.fNEntries == 3
    (cluster,) = rntuple.clusters()
    assert cluster.summary is summary
