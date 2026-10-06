from typing import TYPE_CHECKING, Annotated

from rootfilespec.bootstrap.RAnchor import ROOT3a3aRNTuple
from rootfilespec.rntuple.envelope import (
    ENVELOPE_TYPE_MAP,
    REnvelope,
    REnvelopeLink,
    REnvelopeLocator,
    RFeatureFlags,
)
from rootfilespec.rntuple.pagelist import PageListEnvelope
from rootfilespec.rntuple.RFrame import ListFrame, RecordFrame
from rootfilespec.rntuple.RLocator import RLocator, in_file
from rootfilespec.rntuple.schema import (
    AliasColumnDescription,
    ColumnDescription,
    ExtraTypeInformation,
    FieldDescription,
    RNTupleString,
)
from rootfilespec.serializable import (
    serializable,
)
from rootfilespec.structutil import Fmt, IfBytesRemain

if TYPE_CHECKING:
    from rootfilespec.rntuple.RNTuple import RNTuple

ATTRIBUTE_META_FIELDS = [b"_rangeStart", b"_rangeLen", b"_userData"]
"""The top-level fields of an attribute set of schema version 1.x, in order
(spec, *Attribute Schema Version*)"""


@serializable
class ClusterGroup(RecordFrame):
    """A class representing an RNTuple Cluster Group Record Frame.
    This Record Frame is found in a List Frame in the Footer Envelope of an RNTuple.
    It references the Page List Envelopes for groups of clusters in the RNTuple.
    """

    fMinEntryNumber: Annotated[int, Fmt("<Q")]
    """The minimum of the first entry number across all of the clusters in the group."""
    fEntrySpan: Annotated[int, Fmt("<Q")]
    """The number of entries that are covered by this cluster group."""
    fNClusters: Annotated[int, Fmt("<I")]
    """The number of clusters in the group."""
    pagelistLink: REnvelopeLink
    """Envelope Link to the Page List Envelope for the cluster group."""


@serializable
class SchemaExtension(RecordFrame):
    """A class representing an RNTuple Schema Extension Record Frame.
    This Record Frame is found in the Footer Envelope of an RNTuple.

    The schema extension record frame contains an additional schema description that is incremental with respect to
            the schema contained in the header (see Section Header Envelope). Specifically, it is a record frame with
            the following four fields (identical to the last four fields in Header Envelope):

                List frame: list of field record frames
                List frame: list of column record frames
                List frame: list of alias column record frames
                List frame: list of extra type information

    In general, a schema extension is optional, and thus this record frame might be empty.
        The interpretation of the information contained therein should be identical as if it was found
        directly at the end of the header. This is necessary when fields have been added during writing.

    Note that the field IDs and physical column IDs given by the serialization order should
        continue from the largest IDs found in the header.

    Note that is it possible to extend existing fields by additional column representations.
        This means that columns of the extension header may point to fields of the regular header.

    In practice, deferred columns only appear in the schema extension record frame.
    """

    fieldDescriptions: ListFrame[FieldDescription]
    """The List Frame of Field Description Record Frames. Part of the RNTuple schema description."""
    columnDescriptions: ListFrame[ColumnDescription]
    """The List Frame of Column Description Record Frames. Part of the RNTuple schema description."""
    aliasColumnDescriptions: ListFrame[AliasColumnDescription]
    """The List Frame of Alias Column Description Record Frames. Part of the RNTuple schema description."""
    extraTypeInformations: ListFrame[ExtraTypeInformation]
    """The List Frame of Extra Type Information Record Frames. Part of the RNTuple schema description."""


@serializable
class LinkedAttributeSet(RecordFrame):
    """A class representing an RNTuple Linked Attribute Set Record Frame.
    These Record Frames are found in the last List Frame of the Footer Envelope.

    Each links an attribute set: an RNTuple of its own, holding user metadata
    about ranges of this RNTuple's entries (spec, *Linked Attribute Sets*). Its
    anchor is a ROOT::RNTuple object whose key no directory lists, so this
    record is the only way to it (root-io-spec RNTuple NOTES 8).
    """

    fSchemaVersionMajor: Annotated[int, Fmt("<H")]
    """The major version of the attribute schema, the set's internal fields"""
    fSchemaVersionMinor: Annotated[int, Fmt("<H")]
    """The minor version of the attribute schema"""
    fAnchorLength: Annotated[int, Fmt("<I")]
    """The uncompressed length of the set's anchor: the whole ROOT::RNTuple
    object and its checksum, 78 bytes for class version 2 (root-io-spec RNTuple
    ERRATA 12)"""
    locator: RLocator
    """The locator of the set's anchor: the anchor object, not its key"""
    fName: RNTupleString
    """The name of the attribute set, which is also its RNTuple's name"""

    @property
    def anchor_locator(self) -> REnvelopeLocator[ROOT3a3aRNTuple]:
        """Get a locator for the set's anchor.

        The set is an RNTuple of its own, so it opens like any other:
        ``fetch.rntuple(fetch(record.anchor_locator))``.

        Raises NotImplementedError if the anchor's locator is not in the file.
        ``Fetcher.attribute_set`` opens the set and checks it (``check``).
        """
        return REnvelopeLocator(
            self.fAnchorLength,
            in_file(self.locator, f"The anchor of attribute set {self.fName!r}"),
            ROOT3a3aRNTuple,
        )

    def check(self, rntuple: "RNTuple") -> None:
        """Check that the RNTuple this record links is an attribute set of its version

        As ROOT checks when it opens a set (``RNTupleAttrSetReader``,
        ``RNTupleAttrReading.cxx:20-46`` at 6.40.04), and with the spec's
        restrictions (spec, *Linked Attribute Sets*). Raises NotImplementedError
        for a major schema version other than 1, which this reader doesn't know,
        and ValueError for anything else.
        """
        name = self.fName
        version = f"{self.fSchemaVersionMajor}.{self.fSchemaVersionMinor}"
        # A new major version breaks forward compatibility (spec, *Attribute
        # Schema Version*)
        if self.fSchemaVersionMajor != 1:
            msg = f"Attribute set {name!r} has schema version {version}: only major version 1 is supported"
            raise NotImplementedError(msg)
        schema = rntuple.schemaDescription
        # The three fields of schema 1.x, and no other top-level field: ROOT
        # refuses a fourth whatever the minor version, which the spec says
        # adds fields to ignore (root-io-spec RNTuple ERRATA 13)
        toplevel = [
            field.fFieldName
            for fieldID, field in enumerate(schema.fieldDescriptions)
            if field.fParentFieldID == fieldID
        ]
        if toplevel != ATTRIBUTE_META_FIELDS:
            msg = f"Attribute set {name!r} of schema version {version} has the top-level fields {toplevel}, not {ATTRIBUTE_META_FIELDS}"
            raise ValueError(msg)
        # The spec's restrictions, which ROOT's writer keeps and its reader
        # doesn't check (root-io-spec RNTuple NOTES 8)
        if rntuple.footerEnvelope.attributeSets:
            msg = f"Attribute set {name!r} links attribute sets of its own"
            raise ValueError(msg)
        if schema.aliasColumnDescriptions:
            msg = f"Attribute set {name!r} has alias columns"
            raise ValueError(msg)
        if any(field.fStructuralRole == 0x04 for field in schema.fieldDescriptions):
            msg = (
                f"Attribute set {name!r} has a field of structural role 0x04 (streamer)"
            )
            raise ValueError(msg)


@serializable
class FooterEnvelope(REnvelope):
    """A class representing the RNTuple Footer Envelope payload structure."""

    featureFlags: RFeatureFlags
    """The RNTuple Feature Flags (verify this file can be read)"""
    headerChecksum: Annotated[int, Fmt("<Q")]
    """Checksum of the Header Envelope"""
    schemaExtension: SchemaExtension
    """The Schema Extension Record Frame"""
    clusterGroups: ListFrame[ClusterGroup]
    """The List Frame of Cluster Group Record Frames"""
    attributeSets: Annotated[ListFrame[LinkedAttributeSet] | None, IfBytesRemain()]
    """The List Frame of Linked Attribute Set Record Frames, or None if the
    footer ends before it, as one written before format 1.0.1.0 does: ROOT reads
    it only if bytes remain before the checksum (root-io-spec RNTuple ERRATA 11;
    RNTupleSerialize.cxx:2015-2017 at 6.40.04)"""

    @property
    def pagelist_locators(self) -> list[REnvelopeLocator[PageListEnvelope]]:
        """Get locators for all page lists in this footer."""
        return [
            g.pagelistLink.envelope_locator(PageListEnvelope)
            for g in self.clusterGroups
        ]


ENVELOPE_TYPE_MAP[0x02] = "FooterEnvelope"
