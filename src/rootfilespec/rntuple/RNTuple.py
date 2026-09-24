import dataclasses
from collections.abc import Callable
from math import ceil

from rootfilespec.bootstrap.RAnchor import ROOT3a3aRNTuple
from rootfilespec.rntuple.envelope import RFeatureFlags
from rootfilespec.rntuple.footer import FooterEnvelope
from rootfilespec.rntuple.header import HeaderEnvelope
from rootfilespec.rntuple.pagelist import PageListEnvelope
from rootfilespec.rntuple.pagelocations import RPageDescription
from rootfilespec.rntuple.schema import (
    AliasColumnDescription,
    ColumnDescription,
    ColumnType,
    ExtraTypeInformation,
    FieldDescription,
)
from rootfilespec.serializable import Locator, ReadBuffer, ROOTSerializable


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

    def field_path(self, field_id: int) -> bytes:
        """The qualified name of a field: its ancestors' names and its own, joined by "."

        A field ID is the field's position in the combined list (header first,
        then the footer's schema extension). A top-level field names itself as
        its parent, which ends the walk. "." is forbidden in field names (spec,
        *Naming specification*), so the result is unambiguous. Names are bytes,
        as stored.
        """
        fields = self.fieldDescriptions
        names: list[bytes] = []
        seen: set[int] = set()
        fid = field_id
        while True:
            if not 0 <= fid < len(fields):
                msg = f"Field {field_id}: parent chain reaches field ID {fid}, of {len(fields)} fields"
                raise ValueError(msg)
            if fid in seen:
                msg = (
                    f"Field {field_id}: the parent chain has a cycle at field ID {fid}"
                )
                raise ValueError(msg)
            seen.add(fid)
            field = fields[fid]
            names.append(field.fFieldName)
            if field.fParentFieldID == fid:
                return b".".join(reversed(names))
            fid = field.fParentFieldID


@dataclasses.dataclass
class InterpretablePage:
    """A class representing an interpretable page description.
    It provides the page description, uncompressed size, and column type.
    """

    pageDescription: RPageDescription
    """The RPageDescription object representing the page."""
    uncompressedSize: int
    """The uncompressed size of the page, in bytes."""
    columnType: ColumnType
    """The type of the column this page belongs to, e.g. kInt32, kFloat64, etc."""
    clusterID: int
    """The RNTuple-wide ID of the page's cluster, continuous across cluster groups."""
    columnID: int
    """The ID of the page's physical column (its position in the combined column list)."""
    fieldID: int
    """The ID of the field the column belongs to (``ColumnDescription.fFieldID``)."""
    fieldDescription: FieldDescription
    """The description of that field."""
    fieldPath: bytes
    """The field's qualified name (see ``SchemaDescription.field_path``)."""


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

    @property
    def firstClusterIDs(self) -> list[int]:
        """The ID of the first cluster of each cluster group, in footer order

        Cluster IDs continue across cluster groups: a page list's clusters start
        at the number of clusters in all earlier groups (spec, *Page List
        Envelope*). ``pagelistEnvelopes`` are in the same order as the groups.
        """
        first, out = 0, []
        for group in self.footerEnvelope.clusterGroups:
            out.append(first)
            first += group.fNClusters
        return out

    def get_extended_page_descriptions(
        self,
    ) -> list[list[list[list[InterpretablePage]]]]:
        """All page descriptions, by page-list envelope, cluster, column and page.

        The column list of each cluster has one entry per physical column, in
        column-ID order, as the spec guarantees: a suppressed column is there
        too, with no pages. Each page carries its global cluster ID, its column
        ID and its field.
        """
        schema = self.schemaDescription
        columns = schema.columnDescriptions
        paths: dict[int, bytes] = {}
        if len(self.pagelistEnvelopes) != len(self.footerEnvelope.clusterGroups):
            msg = (
                f"{len(self.pagelistEnvelopes)} page lists for "
                f"{len(self.footerEnvelope.clusterGroups)} cluster groups"
            )
            raise ValueError(msg)
        envelopePages: list[list[list[list[InterpretablePage]]]] = []
        for pagelistEnvelope, group, firstClusterID in zip(
            self.pagelistEnvelopes,
            self.footerEnvelope.clusterGroups,
            self.firstClusterIDs,
            strict=True,
        ):
            if len(pagelistEnvelope.pageLocations) != group.fNClusters:
                msg = (
                    f"Page list of cluster group starting at cluster {firstClusterID} "
                    f"has {len(pagelistEnvelope.pageLocations)} clusters, "
                    f"the group says {group.fNClusters}"
                )
                raise ValueError(msg)
            clusters: list[list[list[InterpretablePage]]] = []
            for index, columnlist in enumerate(pagelistEnvelope.pageLocations):
                clusterID = firstClusterID + index
                clusterPages: list[list[InterpretablePage]] = []
                for columnID, (pagelist, column) in enumerate(
                    zip(columnlist, columns, strict=True)
                ):
                    fieldID = column.fFieldID
                    if fieldID not in paths:
                        paths[fieldID] = schema.field_path(fieldID)
                    clusterPages.append(
                        [
                            InterpretablePage(
                                pageDescription=page_description,
                                uncompressedSize=ceil(
                                    abs(page_description.fNElements)
                                    * column.fBitsOnStorage
                                    / 8
                                ),  # Convert bits to bytes
                                columnType=column.fColumnType,
                                clusterID=clusterID,
                                columnID=columnID,
                                fieldID=fieldID,
                                fieldDescription=schema.fieldDescriptions[fieldID],
                                fieldPath=paths[fieldID],
                            )
                            for page_description in pagelist
                        ]
                    )
                clusters.append(clusterPages)
            envelopePages.append(clusters)
        return envelopePages
