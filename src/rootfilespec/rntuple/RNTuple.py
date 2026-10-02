import dataclasses
from collections.abc import Callable
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
    the column is ``InterpretableColumn.firstElementIndex`` plus this."""
    uncompressedSize: int
    """The size of the page's elements packed for storage, before compression, in bytes."""


@dataclasses.dataclass
class InterpretableColumn:
    """One physical column in one cluster: its element range and its pages

    Like ROOT's ``RClusterDescriptor::RColumnRange`` with its ``RPageRange``,
    and enough on its own to read the column's elements in the cluster. The
    element range is the one ROOT's reader builds
    (``CommitSuppressedColumnRanges`` and ``AddExtendedColumnRanges``,
    ``tree/ntuple/src/RNTupleDescriptor.cxx:880`` and ``:920`` at 6.40.04),
    which is what the spec asks of a reader (*Suppressed Columns*; *Column
    Description* for deferred columns).
    """

    columnID: int
    """The ID of the physical column (its position in the combined column list)."""
    columnDescription: ColumnDescription
    """The description of the column; its field is ``columnDescription.fFieldID``."""
    fieldDescription: FieldDescription
    """The description of the column's field."""
    fieldPath: bytes
    """The field's qualified name (see ``SchemaDescription.field_path``)."""
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
    ``summary.fFirstEntryNumber`` and each column's ``firstElementIndex`` are
    positions in this RNTuple. Everything else belongs to the cluster, which can
    be reused unchanged in another RNTuple: offset columns count from the start
    of the cluster (spec, *Column Description* and *Stdlib Types and
    Collections*).
    """

    clusterID: int
    """The RNTuple-wide ID of the cluster: cluster IDs continue across cluster
    groups (spec, *Page List Envelope*)."""
    clusterGroupID: int
    """The position of the cluster's group in the footer's list of cluster groups."""
    summary: ClusterSummary
    """The cluster's summary, as stored: its first entry and number of entries."""
    columns: list[InterpretableColumn]
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

    def clusters(self) -> list[InterpretableCluster]:
        """Every cluster, in cluster-ID order, with every column's elements and pages

        Each cluster has one entry per physical column, so that each is enough
        on its own to read the column in that cluster; see
        ``InterpretableColumn``.

        Raises ``ValueError`` where the page lists contradict the footer or the
        schema.
        """
        schema = self.schemaDescription
        nColumns = len(schema.columnDescriptions)
        nHeaderColumns = len(self.headerEnvelope.columnDescriptions.items)
        groups = self.footerEnvelope.clusterGroups
        if len(self.pagelistEnvelopes) != len(groups):
            msg = f"{len(self.pagelistEnvelopes)} page lists for {len(groups)} cluster groups"
            raise ValueError(msg)
        columns = _Columns(schema)
        clusters: list[InterpretableCluster] = []
        # The cluster that lists the most columns so far, and how many: only a
        # model extension adds columns, so no later cluster lists fewer
        widestClusterID, widest = 0, 0
        for clusterGroupID, (pagelistEnvelope, group) in enumerate(
            zip(self.pagelistEnvelopes, groups, strict=True)
        ):
            pageLocations = pagelistEnvelope.pageLocations.items
            summaries = pagelistEnvelope.clusterSummaries.items
            if not len(pageLocations) == len(summaries) == group.fNClusters:
                msg = (
                    f"Page list of cluster group {clusterGroupID} has "
                    f"{len(summaries)} cluster summaries and page locations for "
                    f"{len(pageLocations)} clusters, the group says {group.fNClusters}"
                )
                raise ValueError(msg)
            for columnlist, summary in zip(pageLocations, summaries, strict=True):
                clusterID = len(clusters)
                listed = columnlist.items
                if not nHeaderColumns <= len(listed) <= nColumns:
                    msg = (
                        f"Cluster {clusterID} lists {len(listed)} columns; the "
                        f"schema has {nHeaderColumns} in the header and "
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
                clusters.append(
                    InterpretableCluster(
                        clusterID=clusterID,
                        clusterGroupID=clusterGroupID,
                        summary=summary,
                        columns=columns.in_cluster(clusterID, summary, listed),
                    )
                )
        return clusters


@dataclasses.dataclass
class _Column:
    """What ``RNTuple.clusters`` needs to know of a column, the same in every cluster"""

    description: ColumnDescription
    field: FieldDescription
    path: bytes
    representations: list[list[int]]
    """The columns of the column's field, by representation and index"""
    index: int
    """The column's index within its representation"""
    repetitions: int | None
    """For a deferred column of the first representation, its elements per entry"""


class _Columns:
    """The columns of a schema, and how to find their ranges in a cluster"""

    def __init__(self, schema: SchemaDescription):
        fieldColumns = schema.field_columns()
        paths: dict[int, bytes] = {}
        self.columns: list[_Column] = []
        for columnID, description in enumerate(schema.columnDescriptions):
            fieldID = description.fFieldID
            if fieldID not in paths:
                paths[fieldID] = schema.field_path(fieldID)
            representations = fieldColumns[fieldID]
            index = representations[description.fRepresentationIndex].index(columnID)
            repetitions = None
            if description.fFirstElementIndex and description.fRepresentationIndex == 0:
                repetitions = self._repetitions(schema, columnID)
            self.columns.append(
                _Column(
                    description=description,
                    field=schema.fieldDescriptions[fieldID],
                    path=paths[fieldID],
                    representations=representations,
                    index=index,
                    repetitions=repetitions,
                )
            )

    @staticmethod
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

    def in_cluster(
        self,
        clusterID: int,
        summary: ClusterSummary,
        listed: list[PageLocations[RPageDescription]],
    ) -> list[InterpretableColumn]:
        """The columns of one cluster, built in the three steps of ROOT's reader"""
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
                suppressed.append((column.description.fFirstElementIndex or 0) < 0)
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
            for representation in column.representations:
                other = representation[column.index]
                if locations[other] is not None and not suppressed[other]:
                    ranges[columnID] = ranges[other]
                    break
            else:
                msg = (
                    f"Cluster {clusterID}: column {columnID} is suppressed, and no "
                    f"other representation of field {column.path!r} is active"
                )
                raise ValueError(msg)

        # 3. A deferred column covers the whole cluster, the elements before its
        # first stored one being zeros; a later representation copies the range
        # of the first, once that is known
        for columnID, column in enumerate(columns):
            if column.repetitions is not None:
                ranges[columnID] = (
                    summary.fFirstEntryNumber * column.repetitions,
                    summary.fNEntries * column.repetitions,
                )
        for columnID, column in enumerate(columns):
            if column.repetitions is None and column.description.fFirstElementIndex:
                ranges[columnID] = ranges[column.representations[0][column.index]]

        out: list[InterpretableColumn] = []
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
                            * column.description.fBitsOnStorage
                            / 8
                        ),  # Convert bits to bytes
                    )
                )
                nextElement += description.n_elements
            out.append(
                InterpretableColumn(
                    columnID=columnID,
                    columnDescription=column.description,
                    fieldDescription=column.field,
                    fieldPath=column.path,
                    pageLocations=location,
                    suppressed=suppressed[columnID],
                    firstElementIndex=firstElementIndex,
                    nElements=nElements,
                    nZeroElements=nZeroElements,
                    pages=pages,
                )
            )
        return out
