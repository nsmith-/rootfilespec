import dataclasses
import re
from pathlib import Path

import pytest
import tomli  # tomllib, once Python 3.10 is dropped
from skhep_testdata import data_path  # type: ignore[import-not-found]

from rootfilespec.bootstrap import ROOT3a3aRNTuple
from rootfilespec.reader import open_path
from rootfilespec.rntuple.RNTuple import RNTuple, SchemaDescription
from rootfilespec.rntuple.schema import ColumnType

SPEC = Path(__file__).parent.parent / "reference" / "root-io-spec"
DATA = SPEC / "data" / "rntuple"

MULTIPLE_REPRESENTATIONS = "test_multiple_representations_rntuple_v1-0-0-0.root"
MULTIPLE_CLUSTER_GROUPS = "test_multiple_cluster_groups_rntuple_v1-0-0-0.root"
EXTENSION_COLUMNS = "test_extension_columns_rntuple_v1-0-0-0.root"


def _load(path: str | Path) -> RNTuple:
    with open_path(path) as reader:
        keys = reader.keylist().values()
        (key,) = [k for k in keys if k.fClassName == b"ROOT::RNTuple"]
        anchor = reader.fetch(key)
        assert isinstance(anchor, ROOT3a3aRNTuple)
        return RNTuple.from_anchor(anchor, reader.fetch.buffer)


def _ranges(rntuple: RNTuple) -> list[list[tuple[bool, int, int, int, int]]]:
    """(suppressed, firstElementIndex, nElements, nZeroElements, pages) per column, per cluster"""
    return [
        [
            (
                columnRange.suppressed,
                columnRange.firstElementIndex,
                columnRange.nElements,
                columnRange.nZeroElements,
                len(columnRange.pages),
            )
            for columnRange in cluster.columnRanges
        ]
        for cluster in rntuple.clusters()
    ]


def test_suppressed_columns_keep_their_position():
    """A column's position is its column ID, suppressed or not

    In this file the field "real" has two representations, Real32 (column 0)
    and Real16 (column 1), and each cluster suppresses one of them. Before,
    cluster 1's only entry was column 1, at position 0. A suppressed column
    takes the element range of the active one (spec, *Suppressed Columns*).
    """
    rntuple = _load(data_path(MULTIPLE_REPRESENTATIONS))
    assert _ranges(rntuple) == [
        [(False, 0, 1, 0, 1), (True, 0, 1, 0, 0)],
        [(True, 1, 1, 0, 0), (False, 1, 1, 0, 1)],
        [(False, 2, 1, 0, 1), (True, 2, 1, 0, 0)],
    ]
    cluster = list(rntuple.clusters())[1]
    column = cluster.columnRanges[1].column
    assert column.columnDescription.fColumnType == ColumnType.kReal16
    (page,) = cluster.columnRanges[1].pages
    assert page.firstElementInCluster == 0
    suppressed = rntuple.pagelistEnvelopes[0].pageLocations[1][0]
    assert suppressed.elementoffset < 0
    assert cluster.columnRanges[0].pageLocations is suppressed


def test_cluster_ids_continue_across_cluster_groups():
    """Issue #116: 3 cluster groups of 5, 4 and 3 clusters are clusters 0 to 11"""
    clusters = list(_load(data_path(MULTIPLE_CLUSTER_GROUPS)).clusters())
    assert [cluster.clusterID for cluster in clusters] == list(range(12))
    assert [cluster.clusterGroupID for cluster in clusters] == [0] * 5 + [1] * 4 + [
        2
    ] * 3
    assert [cluster.summary.fFirstEntryNumber for cluster in clusters] == [
        *(0, 100, 200, 300, 400),
        *(450, 500, 600, 700),
        *(750, 800, 900),
    ]
    # Column 2 has two elements per entry; its pages count from its cluster's start
    assert [cluster.columnRanges[2].firstElementIndex for cluster in clusters] == [
        2 * cluster.summary.fFirstEntryNumber for cluster in clusters
    ]
    assert {
        cluster.columnRanges[2].pages[0].firstElementInCluster for cluster in clusters
    } == {0}


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


@pytest.mark.skipif(not DATA.exists(), reason="reference/root-io-spec not checked out")
def test_columns_user_class():
    """Each column's field path and type, as root-io-spec's case pins them
    ("<field path>: column type <name> = <value>", at the column's offset)"""
    case = tomli.loads(
        (SPEC / "gen" / "cases" / "rntuple" / "user-class" / "case.toml").read_text()
    )
    pinned = sorted(
        (record["offset"], match[1].encode(), ColumnType(record["value"]))
        for record in case["bytes"]
        if (
            match := re.fullmatch(
                r"(\S+): column type \w+ = 0x[0-9a-f]+", record["name"]
            )
        )
    )
    assert len(pinned) == 19
    rntuple = _load(DATA / "user-class.root")
    columns = rntuple.columns()
    assert [
        (column.fieldPath, column.columnDescription.fColumnType) for column in columns
    ] == [(path, type_) for _, path, type_ in pinned]
    # A std::string has an index column and a character column
    assert [
        column.index for column in columns if column.fieldPath == b"fHit.fLabel"
    ] == [
        0,
        1,
    ]
    for cluster in rntuple.clusters():
        assert [columnRange.column for columnRange in cluster.columnRanges] == columns


def test_field_paths_schema_extension():
    """Field IDs continue from the header into the footer's schema extension"""
    rntuple = _load(data_path(EXTENSION_COLUMNS))
    assert len(rntuple.headerEnvelope.fieldDescriptions) == 1
    schema = rntuple.schemaDescription
    assert [schema.field_path(i) for i in range(4)] == [
        b"int_field",
        b"float_field",
        b"intvec_field",
        b"intvec_field._0",
    ]


@pytest.mark.skipif(not DATA.exists(), reason="reference/root-io-spec not checked out")
def test_field_columns():
    """A field's columns by representation, then by index: a std::string has an
    index and a character column; a record has none"""
    schema = _load(DATA / "user-class.root").schemaDescription
    columns = schema.field_columns()
    assert len(columns) == len(schema.fieldDescriptions)
    paths = [schema.field_path(i) for i in range(len(columns))]
    assert columns[paths.index(b"fHit")] == []
    label = columns[paths.index(b"fHit.fLabel")]
    assert [
        [schema.columnDescriptions[c].fColumnType for c in representation]
        for representation in label
    ] == [[ColumnType.kIndex64, ColumnType.kChar]]
    two = _load(data_path(MULTIPLE_REPRESENTATIONS)).schemaDescription
    assert two.field_columns() == [[[0], [1]]]


def test_field_columns_with_unequal_representations_raise():
    """A field's representations have the same number of columns (spec,
    *Suppressed Columns*)"""
    schema = _load(data_path(MULTIPLE_REPRESENTATIONS)).schemaDescription
    columns = list(schema.columnDescriptions)
    columns.append(dataclasses.replace(columns[1]))
    broken = dataclasses.replace(schema, columnDescriptions=columns)
    with pytest.raises(ValueError, match=r"representations have \[1, 2\] columns"):
        broken.field_columns()


def test_field_columns_with_a_column_of_no_field_raise():
    schema = _load(data_path(MULTIPLE_REPRESENTATIONS)).schemaDescription
    columns = list(schema.columnDescriptions)
    columns[1] = dataclasses.replace(columns[1], fFieldID=5)
    broken = dataclasses.replace(schema, columnDescriptions=columns)
    with pytest.raises(ValueError, match="Column 1 belongs to field 5, of 1 fields"):
        broken.field_columns()


def test_field_columns_with_a_missing_representation_raise():
    rntuple = _load(data_path(EXTENSION_COLUMNS))
    extension = rntuple.footerEnvelope.schemaExtension.columnDescriptions.items
    extension[0] = dataclasses.replace(extension[0], fRepresentationIndex=1)
    with pytest.raises(ValueError, match=r"Field 1 has representations \[1\]"):
        rntuple.clusters()


def test_columns_are_described_once():
    """RNTuple.columns() describes each physical column once, with its field and
    its index among its representation's columns; every cluster refers to them"""
    rntuple = _load(data_path(MULTIPLE_REPRESENTATIONS))
    columns = rntuple.columns()
    assert [
        (
            column.columnID,
            column.fieldPath,
            column.columnDescription.fRepresentationIndex,
            column.index,
        )
        for column in columns
    ] == [(0, b"real", 0, 0), (1, b"real", 1, 0)]
    for cluster in rntuple.clusters():
        assert [columnRange.column for columnRange in cluster.columnRanges] == columns


def test_clusters_are_built_one_at_a_time():
    """clusters() is an iterator: the schema and the number of page lists and
    clusters are checked when it is called, and each cluster's page list when
    the iterator reaches it, after the clusters before it were yielded"""
    rntuple = _load(data_path(EXTENSION_COLUMNS))
    (pagelist,) = rntuple.pagelistEnvelopes
    pagelist.pageLocations.items[2].items.pop()
    clusters = rntuple.clusters()
    assert [next(clusters).clusterID, next(clusters).clusterID] == [0, 1]
    with pytest.raises(ValueError, match="Cluster 2 lists 3 columns"):
        next(clusters)


def test_columns_added_by_model_extension():
    """A cluster committed before the model was extended lists only the columns
    that existed then: here 2 of the 4. The view still has all 4, with the
    ranges ROOT's reader gives them (AddExtendedColumnRanges).

    float_field (column 1) and intvec_field (column 2) are deferred from
    elements 200 and 400: they cover every cluster from element 0, the elements
    before those being zeros with no page (spec, *Column Description*).
    intvec_field._0 (column 3) is inside the vector, so it is not deferred: the
    zero vectors are empty, and it holds no elements before cluster 1.
    """
    rntuple = _load(data_path(EXTENSION_COLUMNS))
    columns = rntuple.schemaDescription.columnDescriptions
    assert [c.fFirstElementIndex for c in columns] == [None, 200, 400, None]
    (pagelist,) = rntuple.pagelistEnvelopes
    assert [len(columns) for columns in pagelist.pageLocations] == [2, 4, 4, 4]
    assert [s.fNEntries for s in pagelist.clusterSummaries] == [350, 117, 84, 49]
    assert _ranges(rntuple) == [
        [
            (False, 0, 350, 0, 2),
            (False, 0, 350, 200, 1),
            (False, 0, 350, 350, 0),
            (False, 0, 0, 0, 0),
        ],
        [
            (False, 350, 117, 0, 1),
            (False, 350, 117, 0, 1),
            (False, 350, 117, 50, 1),
            (False, 0, 134, 0, 1),
        ],
        [
            (False, 467, 84, 0, 1),
            (False, 467, 84, 0, 1),
            (False, 467, 84, 0, 1),
            (False, 134, 168, 0, 1),
        ],
        [
            (False, 551, 49, 0, 1),
            (False, 551, 49, 0, 1),
            (False, 551, 49, 0, 1),
            (False, 302, 98, 0, 1),
        ],
    ]
    clusters = list(rntuple.clusters())
    first = clusters[0]
    assert [
        columnRange.pageLocations is None for columnRange in first.columnRanges
    ] == [
        False,
        False,
        True,
        True,
    ]
    # A first stored page starts after the zeros: float_field's at element 200 of
    # cluster 0, and intvec_field's at element 50 of cluster 1, element 400 of
    # the column
    assert [page.firstElementInCluster for page in first.columnRanges[1].pages] == [200]
    columnRange = clusters[1].columnRanges[2]
    (page,) = columnRange.pages
    assert page.firstElementInCluster == 50
    assert columnRange.firstElementIndex + page.firstElementInCluster == 400


@pytest.mark.parametrize(
    "path",
    [
        *(
            pytest.param(DATA / name, id=name)
            for name in sorted(p.name for p in DATA.glob("*.root"))
        ),
        *(
            pytest.param(name, id=name)
            for name in [
                MULTIPLE_REPRESENTATIONS,
                MULTIPLE_CLUSTER_GROUPS,
                EXTENSION_COLUMNS,
            ]
        ),
    ],
)
def test_every_column_is_complete(path: Path | str):
    """In every cluster: one entry per column; a suppressed column has no pages;
    the pages follow the zeros and each other with no gap; and a column's
    clusters follow each other with no gap, from element 0."""
    if isinstance(path, Path):
        if not path.exists():
            pytest.skip("reference/root-io-spec not checked out")
        rntuple = _load(path)
    else:
        rntuple = _load(data_path(path))
    nColumns = len(rntuple.schemaDescription.columnDescriptions)
    nextElement = [0] * nColumns
    columns = rntuple.columns()
    assert [column.columnID for column in columns] == list(range(nColumns))
    for cluster in rntuple.clusters():
        assert [columnRange.column for columnRange in cluster.columnRanges] == columns
        for columnID, columnRange in enumerate(cluster.columnRanges):
            if columnRange.suppressed:
                assert columnRange.pages == []
                assert columnRange.nZeroElements == 0
            else:
                start = columnRange.nZeroElements
                for page in columnRange.pages:
                    assert page.firstElementInCluster == start
                    start += page.pageDescription.n_elements
                assert start == columnRange.nElements
            assert columnRange.firstElementIndex == nextElement[columnID]
            nextElement[columnID] += columnRange.nElements


def test_page_list_with_a_missing_header_column_raises():
    """Only columns of the schema extension may be missing from a page list"""
    rntuple = _load(data_path(EXTENSION_COLUMNS))
    (pagelist,) = rntuple.pagelistEnvelopes
    pagelist.pageLocations.items[0].items = []
    with pytest.raises(ValueError, match="Cluster 0 lists 0 columns"):
        list(rntuple.clusters())


def test_page_list_with_an_extra_column_raises():
    rntuple = _load(data_path(MULTIPLE_CLUSTER_GROUPS))
    columnlist = rntuple.pagelistEnvelopes[1].pageLocations.items[2]
    columnlist.items.append(columnlist.items[0])
    with pytest.raises(ValueError, match="Cluster 7 lists 4 columns"):
        list(rntuple.clusters())


def test_page_list_with_fewer_columns_than_an_earlier_one_raises():
    """Only a model extension adds columns, so a later cluster cannot list fewer"""
    rntuple = _load(data_path(EXTENSION_COLUMNS))
    (pagelist,) = rntuple.pagelistEnvelopes
    pagelist.pageLocations.items[2].items.pop()
    with pytest.raises(
        ValueError, match="Cluster 2 lists 3 columns, fewer than the 4 of cluster 1"
    ):
        list(rntuple.clusters())


def test_page_lists_must_match_the_cluster_groups():
    rntuple = _load(data_path(MULTIPLE_CLUSTER_GROUPS))
    rntuple.pagelistEnvelopes.pop()
    with pytest.raises(ValueError, match="2 page lists for 3 cluster groups"):
        rntuple.clusters()


def test_page_list_must_have_its_groups_clusters():
    rntuple = _load(data_path(MULTIPLE_CLUSTER_GROUPS))
    pagelist = rntuple.pagelistEnvelopes[1]
    pagelist.pageLocations.items.pop()
    pagelist.clusterSummaries.items.pop()
    with pytest.raises(
        ValueError, match="page locations for 3 clusters, the group says 4"
    ):
        rntuple.clusters()


def test_page_list_must_have_a_summary_per_cluster():
    rntuple = _load(data_path(MULTIPLE_CLUSTER_GROUPS))
    rntuple.pagelistEnvelopes[1].clusterSummaries.items.pop()
    with pytest.raises(ValueError, match="has 3 cluster summaries"):
        rntuple.clusters()


def test_suppressed_column_with_pages_raises():
    """Suppressed columns always have an empty list of pages (spec)"""
    rntuple = _load(data_path(MULTIPLE_REPRESENTATIONS))
    clusters = rntuple.pagelistEnvelopes[0].pageLocations.items
    clusters[1].items[0].items = list(clusters[0].items[0].items)
    with pytest.raises(ValueError, match="column 0 is suppressed but has 1 pages"):
        list(rntuple.clusters())


def test_field_with_every_representation_suppressed_raises():
    """Every field has exactly one active representation in a cluster (spec)"""
    rntuple = _load(data_path(MULTIPLE_REPRESENTATIONS))
    clusters = rntuple.pagelistEnvelopes[0].pageLocations.items
    clusters[1].items[1] = clusters[1].items[0]
    with pytest.raises(ValueError, match="no other representation of field b'real'"):
        list(rntuple.clusters())


def test_deferred_column_whose_pages_disagree_raises():
    """intvec_field starts at element 400 (column description), so in cluster 1
    (elements 350 to 467) its pages start at 400"""
    rntuple = _load(data_path(EXTENSION_COLUMNS))
    rntuple.pagelistEnvelopes[0].pageLocations.items[1].items[2].elementoffset = 401
    with pytest.raises(
        ValueError, match="column 2 has elements 350 to 467, but its pages hold 67"
    ):
        list(rntuple.clusters())


def test_deferred_column_inside_a_collection_raises():
    """The format cannot say how many elements a deferred column inside a
    collection has (spec, *Column Description*)"""
    rntuple = _load(data_path(EXTENSION_COLUMNS))
    extension = rntuple.footerEnvelope.schemaExtension.columnDescriptions.items
    extension[2] = dataclasses.replace(extension[2], fFlags=1, fFirstElementIndex=5)
    with pytest.raises(ValueError, match="inside the collection or variant"):
        rntuple.clusters()


def test_field_path_errors():
    schema = _load(data_path(EXTENSION_COLUMNS)).schemaDescription
    with pytest.raises(ValueError, match="parent chain reaches field ID 9"):
        schema.field_path(9)
    fields = list(schema.fieldDescriptions)
    # _0 -> intvec_field -> _0: a cycle
    fields[2] = dataclasses.replace(fields[2], fParentFieldID=3)
    broken = dataclasses.replace(schema, fieldDescriptions=fields)
    assert isinstance(broken, SchemaDescription)
    with pytest.raises(ValueError, match="cycle"):
        broken.field_path(3)
