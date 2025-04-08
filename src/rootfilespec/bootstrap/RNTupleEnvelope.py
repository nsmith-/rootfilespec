from __future__ import annotations

from dataclasses import dataclass

from ..structutil import DataFetcher, ReadBuffer, ROOTSerializable
from .RNTupleEnvelopeLink import DICTIONARY_ENVELOPE

# DICIONARY_ENVELOPE is to avoid circular imports (see RNTupleEnvelopeLink.py)
from .RNTupleFrame import (
    RNTupleListFrame_ClusterGroups,
    RNTupleListFrame_ClusterSummaries,
    RNTupleListFrame_PageLocations_Clusters,
    RNTupleRecordFrame_SchemaExtension,
)

# Map of envelope type to string for printing
ENVELOPE_TYPE_MAP = {0x01: "Header", 0x02: "Footer", 0x03: "Page List", 0x04: "Unknown"}


@dataclass
class RNTupleFeatureFlags(ROOTSerializable):
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
    def read(cls, buffer: ReadBuffer):
        """Reads the RNTuple Feature Flags from the given buffer.

        Args:
            buffer (ReadBuffer): The buffer to read the RNTuple Feature Flags from.

        Returns:
            RNTupleFeatureFlags: The RNTuple Feature Flags instance.
        """

        # Read the flags from the buffer
        (flags,), buffer = buffer.unpack("<q")  # Signed 64-bit integer

        # There are no feature flags defined for RNTuple yet
        # So abort if any bits are set
        if flags != 0:
            msg = f"Unknown feature flags encountered. int:{flags}; binary:{bin(flags)}"
            raise NotImplementedError(msg)

        return cls(flags), buffer


@dataclass
class RNTupleEnvelope(ROOTSerializable):
    """A class representing the RNTuple envelope structure

    Attributes:
        typeID (int): Envelope type (Header, Footer, Page List, or Unknown)
        length (int): Uncompressed size of the entire envelope (header, payload, unknown, checksum)
        payload (bytes): Envelope payload
        checksum (int): Checksum of the envelope
    """

    typeID: int
    length: int
    payload: (
        RNTupleHeaderEnvelope_payload
        | RNTupleFooterEnvelope_payload
        | RNTuplePageListEnvelope_payload
    )
    unknown: bytes | None
    checksum: int

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """Reads an RNTupleEnvelope from the given buffer."""
        #### Get the first 64bit integer (lengthType) which contains the length and type of the envelope
        # lengthType, buffer = buffer.consume(8)
        (lengthType,), buffer = buffer.unpack("<Q")
        # Envelope type, encoded in the 16 least significant bits
        typeID = lengthType & 0xFFFF
        # Envelope size (uncompressed), encoded in the 48 most significant bits
        length = lengthType >> 16
        # Ensure that the length of the envelope matches the buffer length
        if length - 8 != buffer.__len__():
            msg = f"Length of envelope ({length} minus 8) of type {typeID} does not match buffer length ({buffer.__len__()})"
            raise ValueError(msg)

        #### Get the payload
        # relpos is guaranteed to exist (consider compressed data, abspos doesn't exist)
        payload_start_pos = buffer.relpos

        if typeID == 0x01:  # Header
            payload, buffer = RNTupleHeaderEnvelope_payload.read(buffer)
        elif typeID == 0x02:  # Footer
            payload, buffer = RNTupleFooterEnvelope_payload.read(buffer)
        elif typeID == 0x03:  # Page List
            payload, buffer = RNTuplePageListEnvelope_payload.read(buffer)
        else:
            msg = f"Unknown envelope type: {typeID}"
            raise ValueError(msg)

        #### Consume any unknown trailing information in the envelope
        unknown, buffer = buffer.consume(
            length - 8 - 8 - buffer.relpos + payload_start_pos
        )
        # Unknown Bytes = Payload Size - Payload Bytes Read
        #   Payload Size        = Envelope Size - 8 (for lengthType) - 8 (for checksum)
        #   Payload Bytes Read  = buffer.relpos - payload_start_pos

        #### Get the checksum (appended to envelope when writing to disk)
        (checksum,), buffer = buffer.unpack("<Q")  # Last 8 bytes of the envelope

        return cls(typeID, length, payload, unknown, checksum), buffer

    def get_type(self) -> str:
        """Get the envelope type as a string"""
        # If the typeID is not in the map, raise an error
        if self.typeID not in ENVELOPE_TYPE_MAP:
            msg = f"Unknown envelope type: {self.typeID}"
            raise ValueError(msg)
        return ENVELOPE_TYPE_MAP[self.typeID]


DICTIONARY_ENVELOPE[b"RNTupleEnvelope"] = RNTupleEnvelope


@dataclass
class RNTupleHeaderEnvelope_payload(ROOTSerializable):
    """A class representing the RNTuple Header Envelope payload structure"""


@dataclass
class RNTupleFooterEnvelope_payload(ROOTSerializable):
    """A class representing the RNTuple Footer Envelope payload structure.

    Attributes:
        featureFlags (RNTupleFeatureFlags): The RNTuple Feature Flags (verify this file can be read)
        headerChecksum (int): The checksum of the Header Envelope
        schemaExtensionRecordFrame (RNTupleFrame): The Schema Extension Record Frame
        clusterGroupListFrames (RNTupleListFrame_ClusterGroups): The List Frame of Cluster Group Record Frames
    """

    featureFlags: RNTupleFeatureFlags
    headerChecksum: int
    schemaExtensionRecordFrame: RNTupleRecordFrame_SchemaExtension
    clusterGroupListFrame: RNTupleListFrame_ClusterGroups

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """Reads the RNTuple Footer Envelope Payload from the given buffer."""

        #### Read the feature flags
        featureFlags, buffer = RNTupleFeatureFlags.read(buffer)

        #### Read the header envelope checksum (verify later in RNTuple class read method)
        (headerChecksum,), buffer = buffer.unpack("<Q")

        #### Read the Schema Extension Record Frame

        schemaExtensionRecordFrame, buffer = RNTupleRecordFrame_SchemaExtension.read(
            buffer
        )
        # Verify that we read a record frame
        if schemaExtensionRecordFrame.fheader.fType != 0:
            msg = f"Expected a (schema extension) record frame, but got a frame of type {schemaExtensionRecordFrame.fheader.fType=}"
            raise ValueError(msg)

        #### Read the List Frame of Cluster Group Record Frames

        clusterGroupListFrame, buffer = RNTupleListFrame_ClusterGroups.read(buffer)
        # Verify that we read a list frame
        if clusterGroupListFrame.fheader.fType != 1:
            msg = f"Expected a (cluster group) list frame, but got a frame of type {clusterGroupListFrame.fheader.fType=}"
            raise ValueError(msg)

        return cls(
            featureFlags,
            headerChecksum,
            schemaExtensionRecordFrame,
            clusterGroupListFrame,
        ), buffer

    def get_pagelist(self, fetch_data: DataFetcher) -> list[RNTupleEnvelope]:
        """Get the RNTuple Page List Envelopes from the Footer Envelope.

        Page List Envelope Links are stored in the Cluster Group Record Frames in the Footer Envelope Payload.
        """

        #### Get the Page List Envelopes
        pagelist_envelopes = []  # List of RNTuple Page List Envelopes

        ### Iterate through the Cluster Group Record Frames
        for (
            clusterGroupListFrame
        ) in self.clusterGroupListFrame.clusterGroupRecordFrames:
            ## The cluster group record frame contains other info will be useful later.
            #       i.e. Minimum Entry Number, Entry Span, and Number of Clusters.
            # For now, we only need the Page List Envelope Link.

            # Read the page list envelope
            pagelist_envelope = clusterGroupListFrame.pagelistLink.read_envelope(
                fetch_data
            )
            pagelist_envelopes.append(pagelist_envelope)
        return pagelist_envelopes


@dataclass
class RNTuplePageInfo(ROOTSerializable):
    """A class representing the RNTuple Page Info structure."""


@dataclass
class RNTuplePageListEnvelope_payload(ROOTSerializable):
    """A class representing the RNTuple Page List Envelope payload structure.

    Attributes:
    headerChecksum (int): The checksum of the Header Envelope
    clusterSummaryListFrame (list[RNTupleFrame]): The list of Cluster Summary Record Frames
    page_locations (RNTupleListFrame_PageLocations_Clusters): The triple nested list of page locations
    """

    headerChecksum: int
    clusterSummaryListFrame: RNTupleListFrame_ClusterSummaries
    pageLocationsNestedListFrame: RNTupleListFrame_PageLocations_Clusters

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """Reads the RNTuple Page List Envelope Payload from the given buffer."""
        #### Read the header envelope checksum (verify later in RNTuple class read method)
        (headerChecksum,), buffer = buffer.unpack("<Q")

        #### Read the List Frame of Cluster Summary Record Frames
        clusterSummaryListFrame, buffer = RNTupleListFrame_ClusterSummaries.read(buffer)
        # Verify that we read a list frame
        if clusterSummaryListFrame.fheader.fType != 1:
            msg = f"Expected a (cluster summary) list frame, but got a frame of type {clusterSummaryListFrame.fheader.fType=}"
            raise ValueError(msg)

        #### Read the Nested List Frame of Page Locations
        pageLocationsNestedListFrame, buffer = (
            RNTupleListFrame_PageLocations_Clusters.read(buffer)
        )
        # Verify that we read a list frame
        if pageLocationsNestedListFrame.fheader.fType != 1:
            msg = f"Expected a (page locations) list frame, but got a frame of type {pageLocationsNestedListFrame.fheader.fType=}"
            raise ValueError(msg)

        return cls(
            headerChecksum, clusterSummaryListFrame, pageLocationsNestedListFrame
        ), buffer

    def get_pages(self, fetch_data: DataFetcher) -> list[list[list[RNTuplePageInfo]]]:
        """Get the RNTuple Pages from the Page Locations Nested List Frame."""

        #### Get the Pages

        return self.pageLocationsNestedListFrame.read_list(fetch_data)
