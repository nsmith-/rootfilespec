from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

from rootfilespec.bootstrap.envelopebase import ENVELOPE_TYPE_MAP, REnvelope
from rootfilespec.bootstrap.pagelocations import ClusterLocations
from rootfilespec.bootstrap.RFrame import (
    ClusterGroup,
    ClusterSummary,
    ListFrame,
    SchemaExtension,
)
from rootfilespec.bootstrap.RPage import RPage
from rootfilespec.structutil import (
    DataFetcher,
    Fmt,
    ReadBuffer,
    ROOTSerializable,
    serializable,
)


@dataclass
class RFeatureFlags(ROOTSerializable):
    """A class representing the RNTuple Feature Flags.
    RNTuple Feature Flags appear in the Header and Footer Envelopes.
    This class reads the RNTuple Feature Flags from the buffer.
    It also checks if the flags are set for a given feature.
    It aborts reading when an unknown feature is encountered (unknown bit set).
    """

    flags: int
    """The RNTuple Feature Flags (signed 64-bit integer)"""

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        """Reads the RNTuple Feature Flags from the given buffer."""

        # Read the flags from the buffer
        (flags,), buffer = buffer.unpack("<q")  # Signed 64-bit integer

        # There are no feature flags defined for RNTuple yet
        # So abort if any bits are set
        if flags != 0:
            msg = f"Unknown feature flags encountered. int:{flags}; binary:{bin(flags)}"
            raise NotImplementedError(msg)

        return (flags,), buffer


@serializable
class HeaderEnvelope(REnvelope):
    """A class representing the RNTuple Header Envelope payload structure"""


ENVELOPE_TYPE_MAP[0x01] = "HeaderEnvelope"


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

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        # Read the feature flags
        featureFlags, buffer = RFeatureFlags.read(buffer)

        # Read the header checksum
        (headerChecksum,), buffer = buffer.unpack("<Q")

        # Read the schema extension record frame
        schemaExtension, buffer = SchemaExtension.read(buffer)

        # Read the cluster group list frame
        clusterGroups, buffer = ListFrame.read_as(ClusterGroup, buffer)

        return (featureFlags, headerChecksum, schemaExtension, clusterGroups), buffer

    def get_pagelist(self, fetch_data: DataFetcher) -> list[PageListEnvelope]:
        """Get the RNTuple Page List Envelopes from the Footer Envelope.

        Page List Envelope Links are stored in the Cluster Group Record Frames in the Footer Envelope Payload.
        """

        #### Return the Page List Envelopes
        return [
            g.pagelistLink.read_envelope(fetch_data, PageListEnvelope)
            for g in self.clusterGroups
        ]


ENVELOPE_TYPE_MAP[0x02] = "FooterEnvelope"


@serializable
class PageListEnvelope(REnvelope):
    """A class representing the RNTuple Page List Envelope payload structure."""

    headerChecksum: Annotated[int, Fmt("<Q")]
    """Checksum of the Header Envelope"""
    clusterSummaries: ListFrame[ClusterSummary]
    """The List Frame of Cluster Summary Record Frames"""
    pageLocations: ClusterLocations
    """The Page Locations Triple Nested List Frame"""

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        """Reads the RNTuple Page List Envelope payload from the given buffer."""
        # Read the header checksum
        (headerChecksum,), buffer = buffer.unpack("<Q")

        # Read the cluster summary list frame
        clusterSummaries, buffer = ListFrame.read_as(ClusterSummary, buffer)

        # Read the page locations
        pageLocations, buffer = ClusterLocations.read(buffer)

        return (headerChecksum, clusterSummaries, pageLocations), buffer

    def get_pages(self, fetch_data: DataFetcher):
        """Get the RNTuple Pages from the Page Locations Nested List Frame."""
        #### Get the Page Locations
        page_locations: list[list[list[RPage]]] = []

        for i_column, columnlist in enumerate(self.pageLocations):
            page_locations.append([])
            for i_page, pagelist in enumerate(columnlist):
                page_locations[i_column].append([])
                for page_description in pagelist:
                    # Read the page from the buffer
                    page = page_description.get_page(fetch_data)
                    # Append the page to the list
                    page_locations[i_column][i_page].append(page)

        return page_locations


ENVELOPE_TYPE_MAP[0x03] = "PageListEnvelope"
