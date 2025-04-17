from __future__ import annotations

from dataclasses import dataclass

from ..structutil import (
    DataFetcher,
    ReadBuffer,
    ROOTSerializable,
    # StructClass,
    # sfield,
    # structify,
)
from .RNTupleEnvelopeLink import RNTupleEnvelopeLink, RNTupleLocator


@dataclass
class FrameHeader(ROOTSerializable):
    """Initial header for any RNTuple list or record frame

    Attributes:
        fSize (int): The uncompressed size of the frame.
        fType (int): 0 for a record frame, 1 for a list frame.
        nItems (int): The number of items in the list frame. (Skipped for record frames, no bytes are used)
    """

    fSize: int
    fType: int
    nItems: int | None

    @classmethod
    def read(cls, buffer: ReadBuffer):
        # Load the first 64 bit integer in the buffer to determine the frame size & type.
        # Sign of size determines type of frame. Positive size is a record frame, negative size is a list frame.
        (fSize,), buffer = buffer.unpack("<q")
        fType = 0 if fSize >= 0 else 1  # 0 for record frame, 1 for list frame
        fSize = abs(fSize)

        # If the frame is a list frame, read the number of items in the list frame.
        nItems = None  # Default value for record frames (no bytes to read)
        if fType == 1:  # List frame
            (nItems,), buffer = buffer.unpack("<I")

        return cls(fSize, fType, nItems), buffer


""" Types of record frames used by RNTuple:

    Header Envelope:
        - Field Record Frame
        - Column Record Frame
        - Alias Column Record Frame
        - Extra Type Information Record Frame

        Note that the Header Envelope contains a list frame of these record frames.
        Collectively, these record frames make up the "Schema Description" of the RNTuple.

    Footer Envelope:
        - Schema Extension Record Frame (optional)
            - This record frame has four fields (identical to the Header Envelope record frames)
        - Cluster Group Record Frame

    Page List Envelope:
        - Cluster Summary Record Frame

Record frames are more complex, since their elements can have different types and sizes.
    They require a schema to interpret the frame payload correctly.
        So each record frame RNTuple uses is unique, and needs to be deserialized differently.
"""

########################################################################################################################
# Header Envelope Frames

# add header frames here

########################################################################################################################
# Footer Envelope Frames


@dataclass
class RNTupleRecordFrame_SchemaExtension(ROOTSerializable):
    """A class representing an RNTuple Schema Extension Record Frame.
    This Record Frame is found in the Footer Envelope of an RNTuple.
    It is an extension of the "Schema Description" located in the Header Envelope.
    Currently, this class only reads the basic Record Frame structure and stores the payload.
    The schema description is not yet implemented.

    Attributes:
        fheader (FrameHeader): The size and type of the record frame.
        payload (bytes): The payload of the record frame.
        unknown (bytes | None): Unknown trailing information in the frame.

    """

    """ The schema extension record frame contains an additional schema description that is incremental with respect to
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
    """

    fheader: FrameHeader
    payload: bytes
    unknown: bytes | None

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """Reads the RNTuple Schema Extension Record Frame from the given buffer."""
        #### Read the frame header
        fheader, buffer = FrameHeader.read(buffer)

        #### Read the payload

        # Save the buffer position to check how many bytes were read to check for unknown trailing information
        payload_start_pos = buffer.relpos  # relpos is guaranteed to exist (consider compressed data, abspos doesn't exist)

        # Read the payload of the record frame
        payload, buffer = buffer.consume(fheader.fSize - 8)

        #### Consume any unknown trailing information in the frame

        # NOTE: Since we are just reading all of the bytes in this frame, the code capturing the unknown bytes is redundant.
        #       This will change when we implement actually decoding the contents of this frame.

        unknown, buffer = buffer.consume(
            fheader.fSize - 8 - buffer.relpos + payload_start_pos
        )
        # Unknown Bytes = Payload Size - Payload Bytes Read
        #   Payload Size        = frame length  - 8 bytes for the header
        #   Payload Bytes Read  = buffer.relpos - payload_start_pos

        return cls(fheader, payload, unknown), buffer


@dataclass
class RNTupleListFrame_ClusterGroups(ROOTSerializable):
    """A class representing an RNTuple List Frame of Cluster Groups Record Frames.
    This List Frame is found in the Footer Envelope of an RNTuple.
    It contains a list of Cluster Group Record Frames.

    Attributes:
        fheader (FrameHeader): The size and type of the list frame.
        clusterGroupRecordFrames (list[RNTupleRecordFrame_ClusterGroup]): List of Cluster Group Record Frames.
        unknown (bytes | None): Unknown trailing information in the list frame.
    """

    fheader: FrameHeader
    clusterGroupRecordFrames: list[RNTupleRecordFrame_ClusterGroup]
    unknown: bytes | None

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """Reads the RNTuple List Frame of Cluster Groups Record Frames from the given buffer."""

        #### Read the frame header
        fheader, buffer = FrameHeader.read(buffer)

        #### Read the payload

        # Save the buffer position to check how many bytes were read to check for unknown trailing information
        payload_start_pos = buffer.relpos

        ## Read the list of Cluster Group Record Frames
        # Now the buffer starts at the first Cluster Group Record Frame
        clusterGroupRecordFrames = []
        for _ in range(fheader.nItems):
            clusterGroupRecordFrame, buffer = RNTupleRecordFrame_ClusterGroup.read(
                buffer
            )
            # Verify that we read a record frame
            if clusterGroupRecordFrame.fheader.fType != 0:
                msg = f"Expected a (cluster group) record frame, but got a frame of type {clusterGroupRecordFrame.fheader.fType=}"
                raise ValueError(msg)
            clusterGroupRecordFrames.append(clusterGroupRecordFrame)

        #### Consume any unknown trailing information in the frame
        unknown, buffer = buffer.consume(
            fheader.fSize - 12 - buffer.relpos + payload_start_pos
        )
        # Unknown Bytes = Payload Size - Payload Bytes Read
        #   Payload Size        = frame length  - 12 bytes for the header
        #   Payload Bytes Read  = buffer.relpos - payload_start_pos

        return cls(fheader, clusterGroupRecordFrames, unknown), buffer


@dataclass
class RNTupleRecordFrame_ClusterGroup(ROOTSerializable):
    """A class representing an RNTuple Cluster Group Record Frame.
    This Record Frame is found in a List Frame in the Footer Envelope of an RNTuple.
    It references the Page List Envelopes for groups of clusters in the RNTuple.

    Attributes:
        fheader (FrameHeader): The size and type of the record frame.
        minEntryNumber (int): The minimum of the first entry number across all of the clusters in the group.
        entrySpan (int): The number of entries that are covered by this cluster group.
        nClusters (int): The number of clusters in the group.
        pagelistLink (RNTupleEnvelopeLink): Envelope Link to the Page List Envelope for the cluster group.
        unknown (bytes | None): Unknown trailing information in the frame.
    """

    # Notes:
    # To compute the minimum entry number, take first entry number from all clusters in the cluster group, and take the minimum among these numbers.
    # The entry range allows for finding the right page list for random access requests to entries.
    # The number of clusters information allows for using consistent cluster IDs even if cluster groups are accessed non-sequentially.

    fheader: FrameHeader
    minEntryNumber: int
    entrySpan: int
    nClusters: int
    pagelistLink: RNTupleEnvelopeLink
    unknown: bytes | None

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """Reads the RNTuple Cluster Group Record Frame from the given buffer."""

        #### Read the frame header
        fheader, buffer = FrameHeader.read(buffer)

        #### Read the payload

        # Save the buffer position to check how many bytes were read to check for unknown trailing information
        payload_start_pos = buffer.relpos

        # Read the cluster group integer information
        (minEntryNumber, entrySpan, nClusters), buffer = buffer.unpack("<QQI")

        # Read the envelope link to the Page List Envelope for the cluster group
        pagelistLink, buffer = RNTupleEnvelopeLink.read(buffer)

        #### Consume any unknown trailing information in the frame
        unknown, buffer = buffer.consume(
            fheader.fSize - 8 - buffer.relpos + payload_start_pos
        )
        # Unknown Bytes = Payload Size - Payload Bytes Read
        #   Payload Size        = frame length  - 8 bytes for the header
        #   Payload Bytes Read  = buffer.relpos - payload_start_pos

        return cls(
            fheader, minEntryNumber, entrySpan, nClusters, pagelistLink, unknown
        ), buffer


########################################################################################################################
# Page List Envelope Frames


@dataclass
class RNTupleListFrame_ClusterSummaries(ROOTSerializable):
    """A class representing an RNTuple List Frame of Cluster Summary Record Frames.
    This List Frame is found in the Page List Envelopes of an RNTuple.
    It contains a list of Cluster Summary Record Frames.

    Attributes:
        fheader (FrameHeader): The size and type of the list frame.
        clusterSummaryRecordFrames (list[RNTupleRecordFrame_ClusterSummary]): List of Cluster Summary Record Frames.
        unknown (bytes | None): Unknown trailing information in the list frame.
    """

    fheader: FrameHeader
    clusterSummaryRecordFrames: list[RNTupleRecordFrame_ClusterSummary]
    unknown: bytes | None

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """Reads the RNTuple List Frame of Cluster Summary Record Frames from the given buffer."""

        #### Read the frame header
        fheader, buffer = FrameHeader.read(buffer)

        #### Read the payload

        # Save the buffer position to check how many bytes were read to check for unknown trailing information
        payload_start_pos = buffer.relpos

        ## Read the list of Cluster Summary Record Frames
        # Now the buffer starts at the first Cluster Summary Record Frame
        clusterSummaryRecordFrames = []
        for _ in range(fheader.nItems):
            clusterSummaryRecordFrame, buffer = RNTupleRecordFrame_ClusterSummary.read(
                buffer
            )
            # Verify that we read a record frame
            if clusterSummaryRecordFrame.fheader.fType != 0:
                msg = f"Expected a (cluster summary) record frame, but got a frame of type {clusterSummaryRecordFrame.fheader.fType=}"
                raise ValueError(msg)
            clusterSummaryRecordFrames.append(clusterSummaryRecordFrame)

        #### Consume any unknown trailing information in the frame
        unknown, buffer = buffer.consume(
            fheader.fSize - 12 - buffer.relpos + payload_start_pos
        )
        # Unknown Bytes = Payload Size - Payload Bytes Read
        #   Payload Size        = frame length  - 12 bytes for the header
        #   Payload Bytes Read  = buffer.relpos - payload_start_pos

        return cls(fheader, clusterSummaryRecordFrames, unknown), buffer


@dataclass
class RNTupleRecordFrame_ClusterSummary(ROOTSerializable):
    """A class representing an RNTuple Cluster Summary Record Frame.
    The Cluster Summary Record Frame is found in the Page List Envelopes of an RNTuple.
    The Cluster Summary Record Frame contains the entry range of a cluster.
    The order of Cluster Summaries defines the cluster IDs, starting from
        the first cluster ID of the cluster group that corresponds to the page list.

    Attributes:
        fheader (FrameHeader): The size and type of the record frame.
        firstEntryNumber (int): The first entry number in the cluster.
        nEntries (int): The number of entries in the cluster.
        featureFlag (int): The feature flag for the cluster.
        unknown (bytes | None): Unknown trailing information in the frame.
    """

    # Notes:
    # Flag 0x01 is reserved for a future specification version that will support sharded clusters.
    # The future use of sharded clusters will break forward compatibility and thus introduce a corresponding feature flag.
    # For now, readers should abort when this flag is set. Other flags should be ignored.

    fheader: FrameHeader
    firstEntryNumber: int
    #### Note: nEntries and featureFlag are encoded together in a single 64 bit integer
    nEntries: int  # The 56 least significant bits of the 64 bit integer
    featureFlag: int  # The 8 most significant bits of the 64 bit integer
    unknown: bytes | None

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """Reads the RNTuple Cluster Summary Record Frame from the given buffer."""
        #### Read the frame header
        fheader, buffer = FrameHeader.read(buffer)

        #### Read the payload

        # Save the buffer position to check how many bytes were read to check for unknown trailing information
        payload_start_pos = buffer.relpos

        # Read first entry number
        (firstEntryNumber,), buffer = buffer.unpack("<Q")

        # Read the 64 bit integer containing nEntries and featureFlag
        (nEntries_featureFlag,), buffer = buffer.unpack("<Q")
        nEntries = nEntries_featureFlag & 0xFFFFFFFFFFFFFF  # 56 least significant bits
        featureFlag = (nEntries_featureFlag >> 56) & 0xFF  # 8 most significant bits

        #### Consume any unknown trailing information in the frame
        unknown, buffer = buffer.consume(
            fheader.fSize - 8 - buffer.relpos + payload_start_pos
        )
        # Unknown Bytes = Payload Size - Payload Bytes Read
        #   Payload Size        = frame length  - 8 bytes for the header
        #   Payload Bytes Read  = buffer.relpos - payload_start_pos

        return cls(fheader, firstEntryNumber, nEntries, featureFlag, unknown), buffer


@dataclass
class RNTupleListFrame_PageLocations_Clusters(ROOTSerializable):
    """A class representing the RNTuple Page Locations Cluster (Top-Most) List Frame.
    This List Frame is found in the Page List Envelopes of an RNTuple.
    It is the Top-Most List Frame in the triple nested List Frame of RNTuple page locations.

    Attributes:
        fheader (FrameHeader): The size and type of the list frame.
        columnListFrames (list[RNTupleListFrame_Column]): List of Column (Outer) List Frames.
        unknown (bytes | None): Unknown trailing information in the list frame.

    Notes:

    The page locations are stored in a triple nested list frame ( [top-most[outer[inner[Page Location]]]] ):

    Top-Most List Frame -> Outer List Frame -> Inner List Frame ->  Inner Item
           Clusters     ->      Columns     ->       Pages      ->  Page Location

    Note Page Location is not a record frame. This is the only List Frame item that isn't a Frame.
    Note that there is NO ACTUAL DATA in this envelope! Rather, the page locations are pointers to the actual data!!!
    """

    fheader: FrameHeader
    columnListFrames: list[RNTupleListFrame_PageLocations_Columns]
    unknown: bytes | None

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """Reads the RNTuple Page Locations Cluster (Top-Most) List Frame from the given buffer."""
        #### Read the frame header
        fheader, buffer = FrameHeader.read(buffer)

        #### Read the payload

        # Save the buffer position to check how many bytes were read to check for unknown trailing information
        payload_start_pos = buffer.relpos

        ## Read the list of Page Locations Column List Frames
        # Now the buffer starts at the first Column List Frame
        columnListFrames = []
        for _ in range(fheader.nItems):
            columnListFrame, buffer = RNTupleListFrame_PageLocations_Columns.read(
                buffer
            )
            # Verify that we read a list frame
            if columnListFrame.fheader.fType != 1:
                msg = f"Expected a (page locations: column) list frame, but got a frame of type {columnListFrame.fheader.fType=}"
                raise ValueError(msg)
            columnListFrames.append(columnListFrame)

        #### Consume any unknown trailing information in the frame
        unknown, buffer = buffer.consume(
            fheader.fSize - 12 - buffer.relpos + payload_start_pos
        )
        # Unknown Bytes = Payload Size - Payload Bytes Read
        #   Payload Size        = frame length  - 12 bytes for the header
        #   Payload Bytes Read  = buffer.relpos - payload_start_pos

        return cls(fheader, columnListFrames, unknown), buffer

    def read_list(
        self, fetch_data: DataFetcher
    ) -> list[RNTupleListFrame_PageLocations_Columns]:
        return [
            columnListFrame.read_list(fetch_data)
            for columnListFrame in self.columnListFrames
        ]


@dataclass
class RNTupleListFrame_PageLocations_Columns(ROOTSerializable):
    """A class representing the RNTuple Page Locations Column (Outer) List Frame.
    This List Frame is found in the Page List Envelopes of an RNTuple.
    It is the Outer List Frame in the triple nested List Frame of RNTuple page locations.

    Attributes:
        fheader (FrameHeader): The size and type of the list frame.
        pageListFrames (list[RNTupleListFrame_PageLocations_Pages]): List of Page (Inner) List Frames.
        unknown (bytes | None): Unknown trailing information in the list frame.

    Notes:

    The page locations are stored in a triple nested list frame ( [top-most[outer[inner[Page Location]]]] ):

    Top-Most List Frame -> Outer List Frame -> Inner List Frame ->  Inner Item
           Clusters     ->      Columns     ->       Pages      ->  Page Location

    Note Page Location is not a record frame. This is the only List Frame item that isn't a Frame.
    Note that there is NO ACTUAL DATA in this envelope! Rather, the page locations are pointers to the actual data!!!
    """

    fheader: FrameHeader
    pageListFrames: list[RNTupleListFrame_PageLocations_Pages]
    unknown: bytes | None

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """Reads the RNTuple Page Locations Column (Outer) List Frame from the given buffer."""
        #### Read the frame header
        fheader, buffer = FrameHeader.read(buffer)

        #### Read the payload

        # Save the buffer position to check how many bytes were read to check for unknown trailing information
        payload_start_pos = buffer.relpos

        ## Read the list of Page Locations Page List Frames
        # Now the buffer starts at the first Page List Frame
        pageListFrames = []
        for _ in range(fheader.nItems):
            pageListFrame, buffer = RNTupleListFrame_PageLocations_Pages.read(buffer)
            # Verify that we read a list frame
            if pageListFrame.fheader.fType != 1:
                msg = f"Expected a (page locations: page) list frame, but got a frame of type {pageListFrame.fheader.fType=}"
                raise ValueError(msg)
            pageListFrames.append(pageListFrame)

        #### Consume any unknown trailing information in the frame
        unknown, buffer = buffer.consume(
            fheader.fSize - 12 - buffer.relpos + payload_start_pos
        )
        # Unknown Bytes = Payload Size - Payload Bytes Read
        #   Payload Size        = frame length  - 12 bytes for the header
        #   Payload Bytes Read  = buffer.relpos - payload_start_pos

        return cls(fheader, pageListFrames, unknown), buffer

    def read_list(
        self, fetch_data: DataFetcher
    ) -> list[RNTupleListFrame_PageLocations_Pages]:
        return [
            pageListFrame.read_list(fetch_data) for pageListFrame in self.pageListFrames
        ]


@dataclass
class RNTupleListFrame_PageLocations_Pages(ROOTSerializable):
    """A class representing the RNTuple Page Locations Page (Inner) List Frame.
    This List Frame is found in the Page List Envelopes of an RNTuple.
    It is the Inner List Frame in the triple nested List Frame of RNTuple page locations.

    The Inner List Frame payload is followed by a 64bit signed integer element offset and,
    unless the column is suppressed, the 32bit compression settings.

        See notes below on "Suppressed Columns" for additional details.

    Note that the size of the Inner List Frame (`FrameHeader.fSize`) includes the element offset and compression settings.

    Attributes:
        fheader (FrameHeader): The size and type of the list frame.
        pageLocations (list[RNTuple_PageLocation]): List of Page Locations.
        elementOffset (int): 64bit signed integer element offset.
        isSuppressed (bool): True if the column is suppressed, False otherwise.
        compressionSettings (int | None): 32bit unsigned integer compression settings. (None if column is suppressed)
        unknown (bytes | None): Unknown trailing information in the list frame.

    Notes:

    The page locations are stored in a triple nested list frame ( [top-most[outer[inner[Page Location]]]] ):

    Top-Most List Frame -> Outer List Frame -> Inner List Frame ->  Inner Item
           Clusters     ->      Columns     ->       Pages      ->  Page Location

    Note Page Location is not a record frame. This is the only List Frame item that isn't a Frame.
    Note that there is NO ACTUAL DATA in this envelope! Rather, the page locations are pointers to the actual data!!!
    """

    """ Suppressed Columns:

    If the element offset in the inner list frame is negative (sign bit set), the column is suppressed.
        Writers should write the lowest int64_t value, readers should check for a negative value.
        Suppressed columns always have an empty list of pages.
        Suppressed columns omit the compression settings in the inner list frame.

    Suppressed columns belong to a secondary column representation (see Section "Column Description") that
        is inactive in the current cluster. The number of columns and the absolute values of the element
        offsets of primary and secondary representations are identical. When reading a field of a certain entry,
        this assertion allows for searching the corresponding cluster and column element indexes using any of the
        column representations. It also means that readers need to get the element index offset and the number of
        elements of suppressed columns from the corresponding columns of the primary column representation.

    In every cluster, every field has exactly one primary column representation. All other representations must be suppressed.
        Note that the primary column representation can change from cluster to cluster.
    """

    fheader: FrameHeader
    pageLocations: list[RNTuple_PageLocation]
    elementOffset: int
    isSuppressed: bool
    compressionSettings: int | None
    unknown: bytes | None

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """Reads the RNTuple Page Locations Page (Inner) List Frame from the given buffer."""

        #### Read the frame header
        fheader, buffer = FrameHeader.read(buffer)

        #### Read the payload

        # Save the buffer position to check how many bytes were read to check for unknown trailing information
        payload_start_pos = buffer.relpos

        ## Read the list of Page Locations
        # Now the buffer starts at the first Page Location
        pageLocations = []
        for _ in range(fheader.nItems):
            pageLocation, buffer = RNTuple_PageLocation.read(buffer)
            pageLocations.append(pageLocation)

        # Read the 64bit signed integer element offset
        (elementOffset,), buffer = buffer.unpack("<q")

        # Check if the column is suppressed
        isSuppressed = elementOffset < 0  # True if the sign bit is set

        # Read the 32bit unsigned integer compression settings (if the column is not suppressed)
        compressionSettings = None
        if not isSuppressed:
            (compressionSettings,), buffer = buffer.unpack("<I")

        #### Consume any unknown trailing information in the frame
        unknown, buffer = buffer.consume(
            fheader.fSize - 12 - buffer.relpos + payload_start_pos
        )
        # Unknown Bytes = Payload Size - Payload Bytes Read
        #   Payload Size        = frame length  - 12 bytes for the header
        #   Payload Bytes Read  = buffer.relpos - payload_start_pos

        return cls(
            fheader,
            pageLocations,
            elementOffset,
            isSuppressed,
            compressionSettings,
            unknown,
        ), buffer

    def read_list(self, fetch_data: DataFetcher) -> list[RNTuple_PageLocation]:
        return [
            pageLocation.read_page(fetch_data) for pageLocation in self.pageLocations
        ]


@dataclass
class RNTuple_PageLocation(ROOTSerializable):
    """A class representing an RNTuple Page Location.
    This is the innermost item in the triple nested List Frame of RNTuple page locations.
    It is not a record frame, but rather a pointer to the actual data.

    Attributes:
        nElements (int): The number of elements in the page.
        hasChecksum (bool): True if the page has a checksum, False otherwise.
        locator (RNTupleLocator): The locator for the page.

    Notes:
    Note that we do not need to store the uncompressed size of the page because the uncompressed size is given
    by the number of elements in the page and the element size. We do need, however, the per-column and per-cluster
    element offset in order to read a certain entry range without inspecting the meta-data of all the previous clusters.

    The page locations are stored in a triple nested list frame ( [top-most[outer[inner[Page Location]]]] ):

    Top-Most List Frame -> Outer List Frame -> Inner List Frame ->  Inner Item
           Clusters     ->      Columns     ->       Pages      ->  Page Location

    Note Page Location is not a record frame. This is the only List Frame item that isn't a Frame.
    Note that there is NO ACTUAL DATA in this envelope! Rather, the page locations are pointers to the actual data!!!
    """

    nElements: int
    hasChecksum: bool
    locator: RNTupleLocator

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """Reads the RNTuple Page Location from the given buffer."""

        # nElements and hasChecksum are encoded in a single 32 bit signed integer
        (nElements,), buffer = buffer.unpack("<i")

        # If the sign bit is set, the page has a checksum
        # i.e. XxHash-3 64bit checksum of compressed page data is stored just after the page.
        # Not stored here!!! Stored where the page is stored.
        hasChecksum = nElements < 0  # True if the sign bit is set

        # Get the number of elements in the page
        nElements = abs(nElements)  # Get the absolute value of nElements

        # Read the locator for the page
        locator, buffer = RNTupleLocator.read(buffer)

        return cls(nElements, hasChecksum, locator), buffer

    def read_page(self, fetch_data: DataFetcher) -> ROOTSerializable:
        """Reads the page data from the data source using the locator.
        Pages are wrapped in compression blocks (like envelopes).
        """

        #### Load the (possibly compressed) Page into the buffer
        buffer = self.locator.get_buffer(fetch_data)

        #### If compressed, decompress the page
        compressed = None
        # if self.size != self.length:
        if compressed:
            msg = "Compressed pages are not yet supported"
            raise NotImplementedError(msg)
            # TODO: Implement compressed pages
        # Now the page is uncompressed

        #### Read the page
        page, buffer = buffer.consume(buffer.__len__())

        #### Read the checksum (if the page has a checksum)
        checksum = None
        # # The size does not include the page checksum, so we need to read it separately
        # if self.hasChecksum:
        #     checksum, buffer = buffer.unpack("<Q")
        #     # TODO: Check the checksum

        # Check that the buffer is empty
        if buffer:
            msg = f"RNTuple_PageLocation.read_page: buffer not empty after reading page located at {self.locator=}."
            msg += f"\n{self=}"
            msg += f"\n{compressed=}"
            msg += f"\n{page=}"
            msg += f"\n{checksum=}"
            msg += f"\nBuffer: {buffer}"
            raise ValueError(msg)

        return page
