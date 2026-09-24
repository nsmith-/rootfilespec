import dataclasses
from pathlib import Path

import pytest
from skhep_testdata import data_path  # type: ignore[import-not-found]

from rootfilespec.reader import open_path
from rootfilespec.rntuple.RNTuple import RNTuple, SchemaDescription
from rootfilespec.rntuple.schema import ColumnType

DATA = Path(__file__).parent.parent / "reference" / "root-io-spec" / "data" / "rntuple"


def _load(path: str | Path) -> RNTuple:
    with open_path(path) as reader:
        keylist = reader.keylist()
        (name,) = [
            n for n in keylist if keylist[n].fClassName.fString == b"ROOT::RNTuple"
        ]
        return RNTuple.from_anchor(reader.fetch(keylist[name]), reader.fetch.buffer)


def test_suppressed_columns_keep_their_position():
    """A column's position is its column ID, suppressed or not

    In this file the field "real" has two representations, Real32 (column 0)
    and Real16 (column 1), and each cluster suppresses one of them. Before,
    cluster 1's only entry was column 1, at position 0.
    """
    rntuple = _load(data_path("test_multiple_representations_rntuple_v1-0-0-0.root"))
    (envelope,) = rntuple.get_extended_page_descriptions()
    assert [[len(column) for column in cluster] for cluster in envelope] == [
        [1, 0],
        [0, 1],
        [1, 0],
    ]
    (page,) = envelope[1][1]
    assert page.columnID == 1
    assert page.columnType == ColumnType.kReal16
    suppressed = rntuple.pagelistEnvelopes[0].pageLocations[1][0]
    assert suppressed.elementoffset < 0


def test_cluster_ids_continue_across_cluster_groups():
    """Issue #116: 3 cluster groups of 5, 4 and 3 clusters are clusters 0 to 11"""
    rntuple = _load(data_path("test_multiple_cluster_groups_rntuple_v1-0-0-0.root"))
    assert rntuple.firstClusterIDs == [0, 5, 9]
    ids = [
        {page.clusterID for column in cluster for page in column}
        for envelope in rntuple.get_extended_page_descriptions()
        for cluster in envelope
    ]
    assert ids == [{i} for i in range(12)]
    first_entries = [
        summary.fFirstEntryNumber
        for pagelist in rntuple.pagelistEnvelopes
        for summary in pagelist.clusterSummaries
    ]
    assert first_entries == [0, 100, 200, 300, 400, 450, 500, 600, 700, 750, 800, 900]


@pytest.mark.skipif(not DATA.exists(), reason="reference/root-io-spec not checked out")
def test_field_paths_user_class():
    """Issue #49: qualified field names, as root-io-spec's case for this file names
    them (gen/cases/rntuple/user-class/case.toml)"""
    schema = _load(DATA / "user-class.root").schemaDescription
    names = {i: f.fFieldName for i, f in enumerate(schema.fieldDescriptions)}
    paths = {i: schema.field_path(i) for i in names}
    assert paths[0] == b"fHit"
    assert paths[1] == b"fHit.:_0"
    assert paths[2] == b"fHit.:_0.fBaseId"
    assert paths[13] == b"fHits._0.:_0"
    assert paths[24] == b"fFlavour._0"
    assert paths[26] == b"fCharge._0"
    for i, path in paths.items():
        assert path.rsplit(b".", 1)[-1] == names[i]


def test_field_paths_schema_extension():
    """Field IDs continue from the header into the footer's schema extension"""
    rntuple = _load(data_path("test_extension_columns_rntuple_v1-0-0-0.root"))
    assert len(rntuple.headerEnvelope.fieldDescriptions) == 1
    schema = rntuple.schemaDescription
    assert [schema.field_path(i) for i in range(4)] == [
        b"int_field",
        b"float_field",
        b"intvec_field",
        b"intvec_field._0",
    ]


@pytest.mark.skipif(not DATA.exists(), reason="reference/root-io-spec not checked out")
@pytest.mark.parametrize("name", sorted(p.name for p in DATA.glob("*.root")))
def test_every_page_knows_its_column_and_field(name: str):
    rntuple = _load(DATA / name)
    schema = rntuple.schemaDescription
    for envelope in rntuple.get_extended_page_descriptions():
        for cluster in envelope:
            assert len(cluster) == len(schema.columnDescriptions)
            for columnID, column in enumerate(cluster):
                for page in column:
                    assert page.columnID == columnID
                    field_id = schema.columnDescriptions[columnID].fFieldID
                    assert page.fieldID == field_id
                    assert page.fieldDescription == schema.fieldDescriptions[field_id]
                    assert page.fieldPath == schema.field_path(field_id)


def test_field_path_errors():
    schema = _load(
        data_path("test_extension_columns_rntuple_v1-0-0-0.root")
    ).schemaDescription
    with pytest.raises(ValueError, match="parent chain reaches field ID 9"):
        schema.field_path(9)
    fields = list(schema.fieldDescriptions)
    # _0 -> intvec_field -> _0: a cycle
    fields[2] = dataclasses.replace(fields[2], fParentFieldID=3)
    broken = dataclasses.replace(schema, fieldDescriptions=fields)
    assert isinstance(broken, SchemaDescription)
    with pytest.raises(ValueError, match="cycle"):
        broken.field_path(3)
