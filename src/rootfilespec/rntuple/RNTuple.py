import dataclasses
from math import ceil

from rootfilespec.bootstrap.RAnchor import ROOT3a3aRNTuple
from rootfilespec.buffer import DataFetcher
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


@dataclasses.dataclass
class RNTuple:
    """A class representing an RNTuple."""

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
        return SchemaDescription.from_envelopes(
            self.headerEnvelope, self.footerEnvelope
        )

    # can provide helpers to get page descriptions with different filters, columns/rows/etc.
    def get_extended_page_descriptions(
        self,
    ) -> list[list[list[list[InterpretablePage]]]]:
        """Fetches all pages from the RNTuple, decompressing them if necessary."""
        envelopePages: list[list[list[list[InterpretablePage]]]] = [
            [
                [
                    [
                        InterpretablePage(
                            pageDescription=page_description,
                            uncompressedSize=ceil(
                                abs(page_description.fNElements)
                                * column_description.fBitsOnStorage
                                / 8
                            ),  # Convert bits to bytes
                            columnType=column_description.fColumnType,
                        )
                        for page_description in pagelist
                    ]
                    for pagelist, column_description in zip(
                        columnlist, self.schemaDescription.columnDescriptions
                    )
                ]
                for columnlist in pagelistEnvelope.pageLocations
            ]
            for pagelistEnvelope in self.pagelistEnvelopes
        ]
        return envelopePages
