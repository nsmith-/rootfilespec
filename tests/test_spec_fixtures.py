"""Read the fixtures of the root-io-spec reference (reference/root-io-spec)

Each fixture under ``data/`` exercises one feature of the format and is described,
with byte-level assertions, by the ``case.toml`` of the same name under
``gen/cases/``. The submodule is optional: without it this module is skipped.
"""

from pathlib import Path
from typing import NamedTuple

import pytest

from rootfilespec.bootstrap import TBasket
from rootfilespec.bootstrap.TList import TList
from rootfilespec.bootstrap.TStreamerInfo import ArrayDim, TStreamerBase
from rootfilespec.reader import open_path
from tests.test_read import read_file

DATA = Path(__file__).parent.parent / "reference" / "root-io-spec" / "data"
FIXTURES = sorted(path.relative_to(DATA).as_posix() for path in DATA.rglob("*.root"))

if not FIXTURES:
    pytest.skip(
        "The reference/root-io-spec submodule is not checked out"
        " (git submodule update --init reference/root-io-spec, not recursive)",
        allow_module_level=True,
    )


class ExpectedFailure(NamedTuple):
    issues: tuple[int, ...]
    "Open issues (https://github.com/nsmith-/rootfilespec/issues) that track the failure"
    raises: type[Exception]
    "Type of the error"
    match: str
    "Regular expression to be found in the error message"


# A fixture that fails must be listed here, with exactly the error it fails with,
# and a fixture listed here must fail: fixing an issue means removing its entries
# (or moving them on to the next issue that the fixture runs into).
EXPECTED_FAILURES: dict[str, ExpectedFailure] = {
    "classes/canvas.root": ExpectedFailure((101,), ValueError, "Unknown type TCanvas"),
    "classes/containers.root": ExpectedFailure((101,), ValueError, "Unknown type TMap"),
    "classes/formula.root": ExpectedFailure(
        (72,), NotImplementedError, r"TFormula\.update_members is not implemented"
    ),
    "classes/matrix.root": ExpectedFailure(
        (73,), ValueError, "Unknown type TMatrixTSym3cfloat3e"
    ),
    "classes/tarray.root": ExpectedFailure((107,), ValueError, "Unknown type TArrayL "),
    "serialization/clones-array.root": ExpectedFailure(
        (101,), ValueError, "Unknown type TClonesArray"
    ),
    # fVecArr[2], a fixed array of collections, is read as one collection (#90);
    # after that fHits is a member-wise vector (#70)
    "serialization/collection-forms.root": ExpectedFailure(
        (90, 70), ValueError, "Expected a version in the StdVector header"
    ),
    "serialization/collections.root": ExpectedFailure(
        (70,), NotImplementedError, "Memberwise reading of StdVector not implemented"
    ),
    "serialization/double32.root": ExpectedFailure(
        (41,), NotImplementedError, "Unimplemented format float16"
    ),
    "serialization/element-types.root": ExpectedFailure(
        (90,),
        NotImplementedError,
        "Array length not implemented for TStreamerObjectAnyPointer",
    ),
    "serialization/objects.root": ExpectedFailure(
        (105,), ValueError, "Expected position 109 but got 99"
    ),
    # A bare assert on the end of a column in the member-wise branch of StdMap
    "serialization/pairs.root": ExpectedFailure((70,), AssertionError, "^$"),
    "serialization/pointer-forms.root": ExpectedFailure(
        (91,), ValueError, "85 is not a valid ElementType"
    ),
    "serialization/ref-variants.root": ExpectedFailure(
        (101,), ValueError, "Unknown type TRef "
    ),
    "serialization/references.root": ExpectedFailure(
        (101,), ValueError, "Unknown type TRef "
    ),
    # The TStringLong member of SText is left uninterpreted and its bytes are
    # then read as the next member
    "serialization/stringlong.root": ExpectedFailure(
        (101,), IndexError, "Cannot get slice"
    ),
    # TH1L derives from the missing TArrayL64 (#107); its version word of 0 is
    # also misread as announcing a checksum (#111)
    "serialization/version-zero.root": ExpectedFailure(
        (107, 111), TypeError, r"__init__\(\) missing 36 required positional arguments"
    ),
    "ttree/basket-displacement.root": ExpectedFailure(
        (97,), ValueError, "TBasket header flag 51 not supported"
    ),
    "ttree/branch-clones.root": ExpectedFailure(
        (101,), ValueError, "Unknown type TBranchClones"
    ),
    # Raised by the walker of test_read.py, which does not know what to do with a
    # TBranchSTL, rather than by rootfilespec
    "ttree/branch-first-entry.root": ExpectedFailure(
        (113,), TypeError, "Expected TBranch but got TBranchSTL"
    ),
    "ttree/leaf-truncated.root": ExpectedFailure(
        (41,), NotImplementedError, "Unimplemented format float16"
    ),
    # As for ttree/branch-first-entry.root
    "ttree/split-ptr-collection.root": ExpectedFailure(
        (113,), TypeError, "Expected TBranch but got TBranchSTL"
    ),
    "ttree/tree-index.root": ExpectedFailure(
        (101,), ValueError, "Unknown type TTreeIndex"
    ),
}


def test_expected_failures_exist():
    assert set(EXPECTED_FAILURES) <= set(FIXTURES)


@pytest.mark.parametrize("fixture", FIXTURES)
def test_read_fixture(fixture: str):
    # serialization/streamer-info.root reads end to end since #99 (as does every
    # other fixture with a collection written by ROOT >= 6.36)
    expected = EXPECTED_FAILURES.get(fixture)
    if expected is None:
        # Unlike in test_read.py, a NotImplementedError is a failure here
        read_file(DATA / fixture)
        return
    with pytest.raises(expected.raises, match=expected.match):
        read_file(DATA / fixture)
    issues = ", ".join(f"#{issue}" for issue in expected.issues) or "no issue"
    pytest.xfail(reason=f"{issues}: {expected.match}")


def test_tlist_options():
    """Issue #108: each TList entry is followed by its option string"""
    with open_path(DATA / "serialization/object-tags.root") as reader:
        lst = reader.fetch(reader.keylist()["lst"])
    assert isinstance(lst, TList)
    assert lst.fName.fString == b"lst"
    assert len(lst.items) == 4
    assert lst.options == [b"", b"", b"opt", b""]


def test_streamerelement_maxindex():
    """Issue #92: fMaxIndex is big-endian like everything else"""
    with open_path(DATA / "serialization/arrays.root") as reader:
        info = reader.streamerinfos()[b"Arrays"]
    fixed = info.element(b"fFixed")
    assert fixed.fArrayDim == 1
    assert fixed.fArrayLength == 3
    assert fixed.fMaxIndex == ArrayDim(3, 0, 0, 0, 0)
    grid = info.element(b"fGrid")
    assert grid.fArrayDim == 2
    assert grid.fArrayLength == 4
    assert grid.fMaxIndex == ArrayDim(2, 2, 0, 0, 0)


def test_streamerbase_checksum():
    """Issue #92: a TStreamerBase keeps the checksum of the base in fMaxIndex[1]"""
    with open_path(DATA / "serialization/objects.root") as reader:
        base = reader.streamerinfos()[b"TNamed"].element(b"TObject")
    assert isinstance(base, TStreamerBase)
    assert base.fBaseCheckSum == 0x901BC02D


@pytest.mark.xfail(
    strict=True,
    raises=ValueError,
    reason="#96: TBasket header flag 78 not supported (the header has an fIOBits byte)",
)
def test_basket_iobits():
    """Issue #96: the walker of test_read.py does not read the baskets of a branch
    of a fundamental type, so test_read_fixture cannot see this one
    """
    with open_path(DATA / "ttree/basket-iofeatures.root") as reader:
        # The basket of branch n, see gen/cases/ttree/basket-iofeatures/case.toml
        basket, _ = TBasket.read(reader.fetch.buffer_at(offset=302, size=78))
    assert basket.bheader.fNevBuf == 3
    assert basket.bheader.flag == 0
