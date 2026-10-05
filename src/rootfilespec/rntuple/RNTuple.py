import dataclasses
from collections.abc import Callable, Iterator
from math import ceil

from rootfilespec.bootstrap import BOOTSTRAP_CONTEXT
from rootfilespec.bootstrap.RAnchor import ROOT3a3aRNTuple
from rootfilespec.bootstrap.streamedobject import Ref, read_streamed_item
from rootfilespec.bootstrap.TList import TList
from rootfilespec.bootstrap.TStreamerInfo import TStreamerInfo
from rootfilespec.rntuple.envelope import RFeatureFlags
from rootfilespec.rntuple.footer import FooterEnvelope
from rootfilespec.rntuple.header import HeaderEnvelope
from rootfilespec.rntuple.pagelist import ClusterSummary, PageListEnvelope
from rootfilespec.rntuple.pagelocations import PageLocations, RPageDescription
from rootfilespec.rntuple.schema import (
    AliasColumnDescription,
    ColumnDescription,
    ExtraTypeInformation,
    FieldDescription,
)
from rootfilespec.serializable import (
    BufferContext,
    Locator,
    ReadBuffer,
    ROOTSerializable,
)


@dataclasses.dataclass
class SchemaDescription:
    """A class representing the full schema description of an RNTuple.
    It is a combination of the schema description from the header envelope
    and the schema extension from the footer envelope.
    """

    fieldDescriptions: list[FieldDescription]
    """The full list of field descriptions."""
    columnDescriptions: list[ColumnDescription]
    """The full list of column descriptions."""
    aliasColumnDescriptions: list[AliasColumnDescription]
    """The full list of alias column descriptions."""
    extraTypeInformations: list[ExtraTypeInformation]
    """The full list of extra type information."""

    @classmethod
    def from_envelopes(
        cls, headerEnvelope: HeaderEnvelope, footerEnvelope: FooterEnvelope
    ) -> "SchemaDescription":
        """Creates a SchemaDescription from the header and footer envelopes."""
        # Combine field descriptions
        fieldDescriptions = (
            headerEnvelope.fieldDescriptions.items
            + footerEnvelope.schemaExtension.fieldDescriptions.items
        )

        # Combine column descriptions
        columnDescriptions = (
            headerEnvelope.columnDescriptions.items
            + footerEnvelope.schemaExtension.columnDescriptions.items
        )

        # Combine alias column descriptions
        aliasColumnDescriptions = (
            headerEnvelope.aliasColumnDescriptions.items
            + footerEnvelope.schemaExtension.aliasColumnDescriptions.items
        )

        # Combine extra type information
        extraTypeInformations = (
            headerEnvelope.extraTypeInformations.items
            + footerEnvelope.schemaExtension.extraTypeInformations.items
        )

        return cls(
            fieldDescriptions,
            columnDescriptions,
            aliasColumnDescriptions,
            extraTypeInformations,
        )

    def _field_chain(self, field_id: int) -> list[int]:
        """The IDs of a field and of its ancestors, from the field up to its top-level field

        A field ID is the field's position in the combined list (header first,
        then the footer's schema extension). A top-level field names itself as
        its parent, which ends the walk. Raises on an out-of-range ID or a cycle.
        """
        fields = self.fieldDescriptions
        chain: list[int] = []
        fid = field_id
        while True:
            if not 0 <= fid < len(fields):
                msg = f"Field {field_id}: parent chain reaches field ID {fid}, of {len(fields)} fields"
                raise ValueError(msg)
            if fid in chain:
                msg = (
                    f"Field {field_id}: the parent chain has a cycle at field ID {fid}"
                )
                raise ValueError(msg)
            chain.append(fid)
            if fields[fid].fParentFieldID == fid:
                return chain
            fid = fields[fid].fParentFieldID

    def field_path(self, field_id: int) -> bytes:
        """The qualified name of a field: its ancestors' names and its own, joined by "."

        "." is forbidden in field names (spec, *Naming specification*), so the
        result is unambiguous. Names are bytes, as stored.
        """
        chain = self._field_chain(field_id)
        return b".".join(self.fieldDescriptions[fid].fFieldName for fid in chain[::-1])

    def field_columns(self) -> list[list[list[int]]]:
        """The physical column IDs of every field, by representation and then by index

        ``field_columns()[fieldID][r][i]`` is the ``i``-th column of the field's
        representation ``r``, as ROOT numbers them: in column-ID order within
        each representation (``RNTupleSerialize.cxx:1511`` at 6.40.04). A
        field's representations have the same number of columns, which
        correspond one to one (spec, *Suppressed Columns*). A field with no
        columns has no representations.

        Raises if a field's representation indices are not 0, 1, ..., or if its
        representations have different numbers of columns.
        """
        byField: list[dict[int, list[int]]] = [{} for _ in self.fieldDescriptions]
        for columnID, column in enumerate(self.columnDescriptions):
            if not 0 <= column.fFieldID < len(byField):
                msg = f"Column {columnID} belongs to field {column.fFieldID}, of {len(byField)} fields"
                raise ValueError(msg)
            representations = byField[column.fFieldID]
            representations.setdefault(column.fRepresentationIndex, []).append(columnID)
        out: list[list[list[int]]] = []
        for fieldID, representations in enumerate(byField):
            if sorted(representations) != list(range(len(representations))):
                msg = f"Field {fieldID} has representations {sorted(representations)}"
                raise ValueError(msg)
            ordered = [representations[r] for r in range(len(representations))]
            if len({len(columns) for columns in ordered}) > 1:
                msg = (
                    f"Field {fieldID}: its representations have "
                    f"{[len(columns) for columns in ordered]} columns"
                )
                raise ValueError(msg)
            out.append(ordered)
        return out


@dataclasses.dataclass
class InterpretableColumn:
    """One physical column of the RNTuple, the same in every cluster

    Like ROOT's ``RColumnDescriptor``: the column's description, its field, and
    its place among the field's columns. ``RNTuple.columns()`` lists them, and
    each cluster's ``InterpretableColumnRange`` refers to one.
    """

    columnID: int
    """The ID of the physical column (its position in the combined column list)."""
    columnDescription: ColumnDescription
    """The description of the column, as stored. Its field is ``fFieldID`` and
    its representation ``fRepresentationIndex``."""
    fieldDescription: FieldDescription
    """The description of the column's field."""
    fieldPath: bytes
    """The field's qualified name (see ``SchemaDescription.field_path``)."""
    index: int
    """The column's index among the columns of its representation, as ROOT
    numbers them (``RColumnDescriptor::GetIndex``). Columns of the field's other
    representations with the same index correspond to this one (see
    ``SchemaDescription.field_columns``)."""


@dataclasses.dataclass
class InterpretablePage:
    """One page of a column in a cluster

    Like ROOT's ``RClusterDescriptor::RPageInfoExtended``: the page as stored,
    and where its elements start in the cluster.
    """

    pageDescription: RPageDescription
    """The page's entry in the page list, as stored: its locator, its number of
    elements and whether a checksum follows it."""
    firstElementInCluster: int
    """The index of the page's first element among the column's elements in the
    cluster, as ROOT counts it.

    It belongs to the cluster (see ``InterpretableCluster``); the index within
    the column is ``InterpretableColumnRange.firstElementIndex`` plus this."""
    uncompressedSize: int
    """The size of the page's elements packed for storage, before compression, in bytes."""


@dataclasses.dataclass
class InterpretableColumnRange:
    """One physical column in one cluster: its element range and its pages

    Like ROOT's ``RClusterDescriptor::RColumnRange`` with its ``RPageRange``,
    and enough on its own to read the column's elements in the cluster. The
    element range is the one ROOT's reader builds
    (``CommitSuppressedColumnRanges`` and ``AddExtendedColumnRanges``,
    ``tree/ntuple/src/RNTupleDescriptor.cxx:880`` and ``:920`` at 6.40.04),
    which is what the spec asks of a reader (*Suppressed Columns*; *Column
    Description* for deferred columns).
    """

    column: InterpretableColumn
    """The column, as ``RNTuple.columns()`` lists it."""
    pageLocations: PageLocations[RPageDescription] | None
    """The column's entry in the cluster's page list, as stored: its pages,
    element offset and compression settings.

    ``None`` if the page list has no entry for the column: a cluster committed
    before the model was extended lists only the columns that existed then
    (ROOT's ``SerializePageList``, ``tree/ntuple/src/RNTupleSerialize.cxx:1693``
    at 6.40.04)."""
    suppressed: bool
    """Whether the column is a secondary representation, inactive in this cluster.

    A suppressed column has no pages, and its element range is that of the
    corresponding column of the field's active representation. A column that
    the cluster predates is suppressed if it is deferred and suppressed
    (negative ``fFirstElementIndex``)."""
    firstElementIndex: int
    """The index, within the column, of its first element in this cluster."""
    nElements: int
    """The number of elements of the column in this cluster."""
    nZeroElements: int
    """The number of leading elements that have no page on disk and read as zeros.

    Non-zero only for a deferred column, in the clusters up to its first stored
    element: a reader yields zero bytes for them (spec, *Column Description*).
    The pages hold the remaining ``nElements - nZeroElements``."""
    pages: list[InterpretablePage]
    """The column's pages on disk in this cluster, in order."""


@dataclasses.dataclass
class InterpretableCluster:
    """One cluster: its entries, and every column's elements and pages in it

    Like ROOT's ``RClusterDescriptor``. ``clusterID``,
    ``summary.fFirstEntryNumber`` and each column range's ``firstElementIndex``
    are positions in this RNTuple. Everything else belongs to the cluster, which
    can be reused unchanged in another RNTuple: offset columns count from the
    start of the cluster (spec, *Column Description* and *Stdlib Types and
    Collections*).
    """

    clusterID: int
    """The RNTuple-wide ID of the cluster: cluster IDs continue across cluster
    groups (spec, *Page List Envelope*)."""
    clusterGroupID: int
    """The position of the cluster's group in the footer's list of cluster groups."""
    summary: ClusterSummary
    """The cluster's summary, as stored: its first entry and number of entries."""
    columnRanges: list[InterpretableColumnRange]
    """One per physical column, in column-ID order, as the spec guarantees
    (*Page Locations*): a position is the column ID."""


@dataclasses.dataclass
class RNTuple:
    """A class representing an RNTuple."""

    headerEnvelope: HeaderEnvelope
    footerEnvelope: FooterEnvelope
    pagelistEnvelopes: list[PageListEnvelope]

    @classmethod
    def from_anchor(
        cls,
        anchor: ROOT3a3aRNTuple,
        fetch_data: Callable[[Locator[ROOTSerializable]], ReadBuffer],
    ) -> "RNTuple":
        """Reads the RNTuple from the given anchor."""
        headerEnvelope = anchor.get_header(fetch_data)
        footerEnvelope = anchor.get_footer(fetch_data)

        # Verify header checksum in footer
        if footerEnvelope.headerChecksum != headerEnvelope.checksum:
            msg = f"Header checksum mismatch: {footerEnvelope.headerChecksum} != {headerEnvelope.checksum}"
            raise ValueError(msg)
        pagelistEnvelopes = footerEnvelope.get_pagelists(fetch_data)

        # Verify header checksum in each PageListEnvelope
        for pagelistEnvelope in pagelistEnvelopes:
            if pagelistEnvelope.headerChecksum != headerEnvelope.checksum:
                msg = f"PageListEnvelope header checksum mismatch: {pagelistEnvelope.headerChecksum} != {headerEnvelope.checksum}"
                raise ValueError(msg)

        return cls(headerEnvelope, footerEnvelope, pagelistEnvelopes)

    @property
    def featureFlags(self) -> RFeatureFlags:
        """Returns the logical or of the feature flags from the header and footer envelopes."""
        return self.headerEnvelope.featureFlags | self.footerEnvelope.featureFlags

    @property
    def schemaDescription(self) -> SchemaDescription:
        """Returns the full schema description, from the header envelope but including footer information."""
        return SchemaDescription.from_envelopes(
            self.headerEnvelope, self.footerEnvelope
        )

    def streamer_infos(self) -> dict[bytes, TStreamerInfo]:
        """The TStreamerInfo of each class that streamed (role 0x04) fields need, by class name

        Decoded from the content of every extra type information record with content
        identifier 0 (ROOT writes one, in the footer's schema extension: root-io-spec
        ERRATA 10), whose fContent keeps the bytes. Records with other identifiers are
        ignored, as the spec asks. The content is a TList written as if through a
        pointer (RNTupleSerializer::SerializeStreamerInfos), with nothing after it.
        """
        infos: dict[bytes, TStreamerInfo] = {}
        for record in self.schemaDescription.extraTypeInformations:
            if record.fContentIdentifier != 0:  # kStreamerInfo
                continue
            buffer = ReadBuffer(
                memoryview(record.fContent),
                0,
                BOOTSTRAP_CONTEXT,
                BufferContext(abspos=None),
            )
            streamed, rest = read_streamed_item(buffer)
            if not isinstance(streamed, TList) or rest:
                msg = f"Expected the streamer info content to be one TList, got {streamed!r} and {len(rest)} more bytes"
                raise ValueError(msg)
            for item in streamed.items:
                # A pointee comes back bare or as a Ref; always a Ref after #105
                info = item.obj if isinstance(item, Ref) else item
                if not isinstance(info, TStreamerInfo):
                    msg = f"Expected a TStreamerInfo in the streamer info content, got {info!r}"
                    raise ValueError(msg)
                if info.fName in infos:
                    msg = f"Two TStreamerInfo for class {info.fName!r} in the streamer info content"
                    raise ValueError(msg)
                infos[info.fName] = info
        return infos

    def columns(self) -> list[InterpretableColumn]:
        """Every physical column, in column-ID order, with its field and its place among the field's columns

        They are the same in every cluster, so a caller can, say, store them
        before going through the clusters with ``clusters()``. Raises
        ``ValueError`` where the schema contradicts itself (see
        ``SchemaDescription.field_columns``).
        """
        schema = self.schemaDescription
        return _columns(schema, schema.field_columns())

    def clusters(self) -> Iterator[InterpretableCluster]:
        """Every cluster, in cluster-ID order, with every column's elements and pages

        Each cluster has one range per physical column, enough on its own to read
        the column in that cluster; see ``InterpretableColumnRange``. Clusters
        are built one at a time, as the iterator reaches them, so a caller that
        goes through them in turn holds one cluster's ranges at a time.

        Raises ``ValueError`` where the page lists contradict the footer or the
        schema. The schema, and the number of page lists and of their clusters,
        are checked when this is called; each cluster's page list, when the
        iterator reaches it.
        """
        return _ClusterReader(self).clusters()


def _columns(
    schema: SchemaDescription, fieldColumns: list[list[list[int]]]
) -> list[InterpretableColumn]:
    """The physical columns of a schema, with their fields and indices"""
    paths: dict[int, bytes] = {}
    columns: list[InterpretableColumn] = []
    for columnID, description in enumerate(schema.columnDescriptions):
        fieldID = description.fFieldID
        if fieldID not in paths:
            paths[fieldID] = schema.field_path(fieldID)
        representation = fieldColumns[fieldID][description.fRepresentationIndex]
        columns.append(
            InterpretableColumn(
                columnID=columnID,
                columnDescription=description,
                fieldDescription=schema.fieldDescriptions[fieldID],
                fieldPath=paths[fieldID],
                index=representation.index(columnID),
            )
        )
    return columns


def _repetitions(schema: SchemaDescription, columnID: int) -> int:
    """The elements per entry of a deferred column of the first representation

    The product of the array sizes of its field and its ancestors. The spec
    allows an unsuppressed deferred column only where no ancestor is a
    collection or a variant (*Column Description*); there, the number of
    elements cannot be known.
    """
    fields = schema.fieldDescriptions
    fieldID = schema.columnDescriptions[columnID].fFieldID
    chain = schema._field_chain(fieldID)
    for fid in chain[1:]:
        if fields[fid].fStructuralRole in (0x01, 0x03):  # collection, variant
            msg = (
                f"Column {columnID} is deferred, but its field "
                f"{schema.field_path(fieldID)!r} is inside the collection or "
                f"variant {schema.field_path(fid)!r}"
            )
            raise ValueError(msg)
    repetitions = 1
    for fid in chain:
        repetitions *= max(fields[fid].fArraySize or 0, 1)
    return repetitions


class _ClusterReader:
    """Builds the clusters of an RNTuple one at a time, from what is computed once"""

    def __init__(self, rntuple: RNTuple):
        schema = rntuple.schemaDescription
        fieldColumns = schema.field_columns()
        self.columns = _columns(schema, fieldColumns)
        # The columns of each column's field, by representation and index
        self.representations = [
            fieldColumns[column.columnDescription.fFieldID] for column in self.columns
        ]
        # The elements per entry of each deferred column of the first representation
        self.repetitions = [
            _repetitions(schema, column.columnID)
            if column.columnDescription.fFirstElementIndex
            and column.columnDescription.fRepresentationIndex == 0
            else None
            for column in self.columns
        ]
        self.nHeaderColumns = len(rntuple.headerEnvelope.columnDescriptions.items)
        groups = rntuple.footerEnvelope.clusterGroups
        if len(rntuple.pagelistEnvelopes) != len(groups):
            msg = f"{len(rntuple.pagelistEnvelopes)} page lists for {len(groups)} cluster groups"
            raise ValueError(msg)
        for clusterGroupID, (pagelistEnvelope, group) in enumerate(
            zip(rntuple.pagelistEnvelopes, groups, strict=True)
        ):
            nLocations = len(pagelistEnvelope.pageLocations.items)
            nSummaries = len(pagelistEnvelope.clusterSummaries.items)
            if not nLocations == nSummaries == group.fNClusters:
                msg = (
                    f"Page list of cluster group {clusterGroupID} has "
                    f"{nSummaries} cluster summaries and page locations for "
                    f"{nLocations} clusters, the group says {group.fNClusters}"
                )
                raise ValueError(msg)
        self.pagelistEnvelopes = rntuple.pagelistEnvelopes

    def clusters(self) -> Iterator[InterpretableCluster]:
        nColumns = len(self.columns)
        clusterID = 0
        # The cluster that lists the most columns so far, and how many: only a
        # model extension adds columns, so no later cluster lists fewer
        widestClusterID, widest = 0, 0
        for clusterGroupID, pagelistEnvelope in enumerate(self.pagelistEnvelopes):
            for columnlist, summary in zip(
                pagelistEnvelope.pageLocations.items,
                pagelistEnvelope.clusterSummaries.items,
                strict=True,
            ):
                listed = columnlist.items
                if not self.nHeaderColumns <= len(listed) <= nColumns:
                    msg = (
                        f"Cluster {clusterID} lists {len(listed)} columns; the "
                        f"schema has {self.nHeaderColumns} in the header and "
                        f"{nColumns} in all"
                    )
                    raise ValueError(msg)
                if len(listed) < widest:
                    msg = (
                        f"Cluster {clusterID} lists {len(listed)} columns, fewer "
                        f"than the {widest} of cluster {widestClusterID} before it"
                    )
                    raise ValueError(msg)
                if len(listed) > widest:
                    widestClusterID, widest = clusterID, len(listed)
                yield InterpretableCluster(
                    clusterID=clusterID,
                    clusterGroupID=clusterGroupID,
                    summary=summary,
                    columnRanges=self._ranges(clusterID, summary, listed),
                )
                clusterID += 1

    def _ranges(
        self,
        clusterID: int,
        summary: ClusterSummary,
        listed: list[PageLocations[RPageDescription]],
    ) -> list[InterpretableColumnRange]:
        """The column ranges of one cluster, built in the three steps of ROOT's reader"""
        columns = self.columns
        locations: list[PageLocations[RPageDescription] | None] = [
            *listed,
            *[None] * (len(columns) - len(listed)),
        ]

        # 1. The page list: an unsuppressed column starts at its element offset
        # and holds its pages' elements; a column the cluster predates holds none
        suppressed: list[bool] = []
        ranges: list[tuple[int, int] | None] = []
        for columnID, (location, column) in enumerate(
            zip(locations, columns, strict=True)
        ):
            if location is None:
                suppressed.append(
                    (column.columnDescription.fFirstElementIndex or 0) < 0
                )
                ranges.append((0, 0))
            elif location.elementoffset < 0:
                if location.items:
                    msg = (
                        f"Cluster {clusterID}: column {columnID} is suppressed "
                        f"but has {len(location.items)} pages"
                    )
                    raise ValueError(msg)
                suppressed.append(True)
                ranges.append(None)
            else:
                suppressed.append(False)
                nStored = sum(page.n_elements for page in location.items)
                ranges.append((location.elementoffset, nStored))

        # 2. A suppressed column takes the range of the corresponding column of
        # the field's active representation (spec, *Suppressed Columns*)
        for columnID, column in enumerate(columns):
            if ranges[columnID] is not None:
                continue
            for representation in self.representations[columnID]:
                other = representation[column.index]
                if locations[other] is not None and not suppressed[other]:
                    ranges[columnID] = ranges[other]
                    break
            else:
                msg = (
                    f"Cluster {clusterID}: column {columnID} is suppressed, and no "
                    f"other representation of field {column.fieldPath!r} is active"
                )
                raise ValueError(msg)

        # 3. A deferred column covers the whole cluster, the elements before its
        # first stored one being zeros; a later representation copies the range
        # of the first, once that is known
        for columnID, repetitions in enumerate(self.repetitions):
            if repetitions is not None:
                ranges[columnID] = (
                    summary.fFirstEntryNumber * repetitions,
                    summary.fNEntries * repetitions,
                )
        for columnID, column in enumerate(columns):
            if (
                self.repetitions[columnID] is None
                and column.columnDescription.fFirstElementIndex
            ):
                first = self.representations[columnID][0][column.index]
                ranges[columnID] = ranges[first]

        out: list[InterpretableColumnRange] = []
        for columnID, (location, column) in enumerate(
            zip(locations, columns, strict=True)
        ):
            range_ = ranges[columnID]
            assert range_ is not None
            firstElementIndex, nElements = range_
            descriptions: list[RPageDescription] = []
            nZeroElements = 0
            if location is not None and not suppressed[columnID]:
                descriptions = location.items
                nStored = sum(page.n_elements for page in descriptions)
                nZeroElements = nElements - nStored
                if (
                    nZeroElements < 0
                    or location.elementoffset != firstElementIndex + nZeroElements
                ):
                    msg = (
                        f"Cluster {clusterID}: column {columnID} has elements "
                        f"{firstElementIndex} to {firstElementIndex + nElements}, "
                        f"but its pages hold {nStored} elements from "
                        f"{location.elementoffset}"
                    )
                    raise ValueError(msg)
            elif not suppressed[columnID]:
                nZeroElements = nElements
            pages: list[InterpretablePage] = []
            nextElement = nZeroElements
            for description in descriptions:
                pages.append(
                    InterpretablePage(
                        pageDescription=description,
                        firstElementInCluster=nextElement,
                        uncompressedSize=ceil(
                            description.n_elements
                            * column.columnDescription.fBitsOnStorage
                            / 8
                        ),  # Convert bits to bytes
                    )
                )
                nextElement += description.n_elements
            out.append(
                InterpretableColumnRange(
                    column=column,
                    pageLocations=location,
                    suppressed=suppressed[columnID],
                    firstElementIndex=firstElementIndex,
                    nElements=nElements,
                    nZeroElements=nZeroElements,
                    pages=pages,
                )
            )
        return out
