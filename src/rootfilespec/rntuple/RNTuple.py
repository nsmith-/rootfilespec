import dataclasses

from rootfilespec.bootstrap.RAnchor import ROOT3a3aRNTuple
from rootfilespec.buffer import DataFetcher
from rootfilespec.rntuple.envelope import RFeatureFlags
from rootfilespec.rntuple.footer import FooterEnvelope
from rootfilespec.rntuple.header import HeaderEnvelope
from rootfilespec.rntuple.pagelist import PageListEnvelope
from rootfilespec.rntuple.schema import AliasColumnDescription, ColumnDescription, ExtraTypeInformation, FieldDescription


@dataclasses.dataclass
class SchemaDescription():
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
    def from_envelopes(cls, headerEnvelope: HeaderEnvelope, footerEnvelope: FooterEnvelope) -> "SchemaDescription":
        """Creates a SchemaDescription from the header and footer envelopes."""
        # Combine field descriptions
        fieldDescriptions = headerEnvelope.fieldDescriptions.items + footerEnvelope.schemaExtension.fieldDescriptions.items

        # Combine column descriptions
        columnDescriptions = headerEnvelope.columnDescriptions.items + footerEnvelope.schemaExtension.columnDescriptions.items

        # Combine alias column descriptions
        aliasColumnDescriptions = headerEnvelope.aliasColumnDescriptions.items + footerEnvelope.schemaExtension.aliasColumnDescriptions.items

        # Combine extra type information
        extraTypeInformations = headerEnvelope.extraTypeInformations.items + footerEnvelope.schemaExtension.extraTypeInformations.items

        return cls(fieldDescriptions, columnDescriptions, aliasColumnDescriptions, extraTypeInformations)

@dataclasses.dataclass
class RNTuple:
    """A class representing an RNTuple.

    # abbott: what do we need this to do?
    - Provide method to decompress pages, maybe method for getting specific page
    - Provide method to deserialize pages (or something close)?
    """

    headerEnvelope: HeaderEnvelope
    footerEnvelope: FooterEnvelope
    pagelistEnvelopes: list[PageListEnvelope]

    @classmethod
    def from_anchor(cls, anchor: ROOT3a3aRNTuple, fetch_data: DataFetcher) -> "RNTuple":
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
        return SchemaDescription.from_envelopes(self.headerEnvelope, self.footerEnvelope)
