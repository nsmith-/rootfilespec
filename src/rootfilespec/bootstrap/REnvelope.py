from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated

from typing_extensions import Self

from rootfilespec.bootstrap.RFrame import (
    ClusterGroup,
    ClusterSummary,
    ListFrame,
    PageLocations_Clusters,
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

# Map of envelope type to string for printing
ENVELOPE_TYPE_MAP = {
    0x00: "Reserved",
    0x01: "Header",
    0x02: "Footer",
    0x03: "Page List",
}


@dataclass
class RFeatureFlags(ROOTSerializable):
    """A class representing the RNTuple Feature Flags.
    RNTuple Feature Flags appear in the Header and Footer Envelopes.
    This class reads the RNTuple Feature Flags from the buffer.
    It also checks if the flags are set for a given feature.
    It aborts reading when an unknown feature is encountered (unknown bit set).

    Attributes:
        flags (int): The RNTuple Feature Flags (signed 64-bit integer)
    """

    flags: int

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


@dataclass
class REnvelope(ROOTSerializable):
    """A class representing the RNTuple Envelope.
    An RNTuple Envelope is a data block that contains information about the RNTuple data.
    The following envelope types exist
    - Header Envelope (0x01): RNTuple schema information (field and column types)
    - Footer Envelope (0x02): Description of clusters
    - Page List Envelope (0x03): Location of data pages
    - Reserved (0x00): Unused and Reserved

    """

    typeID: int
    length: int
    checksum: int
    _unknown: bytes = field(init=False, repr=False)

    @classmethod
    def read(cls, buffer: ReadBuffer) -> tuple[Self, ReadBuffer]:
        """Reads an REnvelope from the given buffer."""
        #### Save initial buffer position (for checking unknown bytes)
        payload_start_pos = buffer.relpos

        #### Get the first 64bit integer (lengthType) which contains the length and type of the envelope
        # lengthType, buffer = buffer.consume(8)
        (lengthType,), buffer = buffer.unpack("<Q")
        # Envelope type, encoded in the 16 least significant bits
        typeID = lengthType & 0xFFFF
        # Envelope size (uncompressed), encoded in the 48 most significant bits
        length = lengthType >> 16
        # Ensure that the length of the envelope matches the buffer length
        if length - 8 != len(buffer):
            msg = f"Length of envelope ({length} minus 8) of type {typeID} does not match buffer length ({len(buffer)})"
            raise ValueError(msg)

        #### Get the payload
        cls_args, buffer = cls.read_members(buffer)

        #### Consume any unknown trailing information in the envelope
        _unknown, buffer = buffer.consume(
            length - (buffer.relpos - payload_start_pos) - 8
        )
        # Unknown Bytes = Envelope Size - Envelope Bytes Read - Checksum (8 bytes)
        #   Envelope Bytes Read  = buffer.relpos - payload_start_pos

        #### Get the checksum (appended to envelope when writing to disk)
        (checksum,), buffer = buffer.unpack("<Q")  # Last 8 bytes of the envelope

        envelope = cls(typeID, length, checksum, *cls_args)
        envelope._unknown = _unknown
        return envelope, buffer

    def get_type(self) -> str:
        """Get the envelope type as a string"""
        # If the typeID is not in the map, raise an error
        if self.typeID not in ENVELOPE_TYPE_MAP:
            msg = f"Unknown envelope type: {self.typeID}"
            raise ValueError(msg)
        return ENVELOPE_TYPE_MAP[self.typeID]


@serializable
class HeaderEnvelope(REnvelope):
    """A class representing the RNTuple Header Envelope payload structure"""


@serializable
class FooterEnvelope(REnvelope):
    """A class representing the RNTuple Footer Envelope payload structure.

    Attributes:
        featureFlags (RFeatureFlags): The RNTuple Feature Flags (verify this file can be read)
        headerChecksum (int): Checksum of the Header Envelope
        schemaExtension (SchemaExtension): The Schema Extension Record Frame
        clusterGroups (ListFrame[ClusterGroup]): The List Frame of Cluster Group Record Frames
    """

    featureFlags: RFeatureFlags
    headerChecksum: Annotated[int, Fmt("<Q")]
    schemaExtension: SchemaExtension
    clusterGroups: ListFrame[ClusterGroup]

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

        #### Get the Page List Envelopes
        pagelist_envelopes = []  # List of RNTuple Page List Envelopes

        ### Iterate through the Cluster Group Record Frames
        for _i, clusterGroup in enumerate(self.clusterGroups):
            ## The cluster group record frame contains other info will be useful later.
            #       i.e. Minimum Entry Number, Entry Span, and Number of Clusters.
            # For now, we only need the Page List Envelope Link.

            # Read the page list envelope
            pagelist_envelope = clusterGroup.pagelistLink.read_envelope(
                fetch_data, PageListEnvelope
            )
            pagelist_envelopes.append(pagelist_envelope)
        return pagelist_envelopes


@serializable
class PageListEnvelope(REnvelope):
    """A class representing the RNTuple Page List Envelope payload structure.

    Attributes:
    headerChecksum (int): Checksum of the Header Envelope
    clusterSummaries (ListFrame[ClusterSummary]): List Frame of Cluster Summary Record Frames
    pageLocations (PageLocations_Clusters): The Page Locations Triple Nested List Frame
    """

    headerChecksum: Annotated[int, Fmt("<Q")]
    clusterSummaries: ListFrame[ClusterSummary]
    pageLocations: PageLocations_Clusters

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        """Reads the RNTuple Page List Envelope payload from the given buffer."""
        # Read the header checksum
        (headerChecksum,), buffer = buffer.unpack("<Q")

        # Read the cluster summary list frame
        clusterSummaries, buffer = ListFrame.read_as(ClusterSummary, buffer)

        # Read the page locations
        pageLocations, buffer = PageLocations_Clusters.read(buffer)

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
