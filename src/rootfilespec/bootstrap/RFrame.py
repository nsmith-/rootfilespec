from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Any, Generic, TypeVar

from typing_extensions import Self

from rootfilespec.bootstrap.REnvelopeLink import (
    REnvelopeLink,
    RLocator,
)
from rootfilespec.bootstrap.RPage import RPage
from rootfilespec.structutil import (
    DataFetcher,
    Fmt,
    ReadBuffer,
    ROOTSerializable,
    serializable,
)

Item = TypeVar("Item", bound=ROOTSerializable)


@dataclass
class RFrame(ROOTSerializable):
    fSize: int  # abbott Q: do we need to save fSize?
    _unknown: bytes = field(init=False, repr=False)

    @classmethod
    def read_as(
        cls, itemtype: type[Item], buffer: ReadBuffer
    ) -> tuple[Any, ReadBuffer]:
        # Peek at the metadata to determine the type of frame but don't consume it
        (fSize,), _ = buffer.unpack("<q")

        # If the size is positive, it's a Record Frame
        if fSize > 0:
            return RecordFrame.read_as(itemtype, buffer)
        # If the size is negative, it's a List Frame
        if fSize < 0:
            return ListFrame.read_as(itemtype, buffer)
        # If the size is zero, something is wrong
        msg = f"Expected metadata to be non-zero, but got {fSize=}"
        raise ValueError(msg)


@dataclass
class ListFrame(RFrame, Generic[Item]):
    items: list[Item]

    @classmethod
    def read_as(
        cls,
        itemtype: type[Item],  # type: ignore[override]
        buffer: ReadBuffer,
    ) -> tuple[Self, ReadBuffer]:
        # Save initial buffer position (for checking unknown bytes)
        start_position = buffer.relpos

        #### Read the frame Size and Type
        (fSize,), buffer = buffer.unpack("<q")
        if fSize >= 0:
            msg = f"Expected fSize to be negative, but got {fSize}"
            raise ValueError(msg)
        # abs(fSize) is the uncompressed byte size of frame (including payload)
        fSize = abs(fSize)

        #### Read the List Frame Items
        (nItems,), buffer = buffer.unpack("<I")
        items: list[Item] = []
        while len(items) < nItems:
            # Read a regular item
            item, buffer = itemtype.read(buffer)
            items.append(item)

        cls_args, buffer = cls.read_members(buffer)

        #### Consume any unknown trailing information in the frame
        _unknown, buffer = buffer.consume(fSize - (buffer.relpos - start_position))
        # Unknown Bytes = Frame Size - Bytes Read
        # Bytes Read = buffer.relpos - start_position

        frame = cls(fSize, items, *cls_args)
        frame._unknown = _unknown
        return frame, buffer

    @classmethod
    def read_members(cls, buffer: ReadBuffer) -> tuple[tuple[Any, ...], ReadBuffer]:
        """Reads extra members from the buffer. This is a placeholder for subclasses to implement."""
        # For now, just return an empty tuple and the buffer unchanged
        return (), buffer

    def __len__(self):
        return len(self.items)

    def __iter__(self):
        return iter(self.items)

    def __getitem__(self, index: int) -> Item:
        return self.items[index]


@dataclass
class RecordFrame(RFrame):
    @classmethod
    def read(cls, buffer: ReadBuffer):
        #### Save initial buffer position (for checking unknown bytes)
        start_position = buffer.relpos

        #### Read the frame Size and Type
        (fSize,), buffer = buffer.unpack("<q")
        if fSize <= 0:
            msg = f"Expected fSize to be positive, but got {fSize}"
            raise ValueError(msg)

        #### Read the Record Frame Payload
        args, buffer = cls.read_members(buffer)

        # abbott: any checks here?

        #### Consume any unknown trailing information in the frame
        _unknown, buffer = buffer.consume(fSize - (buffer.relpos - start_position))
        # Unknown Bytes = Frame Size - Bytes Read
        # Bytes Read = buffer.relpos - start_position

        frame = cls(fSize, *args)
        frame._unknown = _unknown
        return frame, buffer


########################################################################################################################
# Header Envelope Frames

########################################################################################################################
# Footer Envelope Frames


@serializable
class ClusterGroup(RecordFrame):
    """A class representing an RNTuple Cluster Group Record Frame.
    This Record Frame is found in a List Frame in the Footer Envelope of an RNTuple.
    It references the Page List Envelopes for groups of clusters in the RNTuple.

    Attributes:
        fMinEntryNumber (int): The minimum of the first entry number across all of the clusters in the group.
        fEntrySpan (int): The number of entries that are covered by this cluster group.
        fNClusters (int): The number of clusters in the group.
        pagelistLink (REnvelopeLink): Envelope Link to the Page List Envelope for the cluster group.
    """

    fMinEntryNumber: Annotated[int, Fmt("<Q")]
    fEntrySpan: Annotated[int, Fmt("<Q")]
    fNClusters: Annotated[int, Fmt("<I")]
    pagelistLink: REnvelopeLink


@serializable
class SchemaExtension(RecordFrame):
    """A class representing an RNTuple Schema Extension Record Frame.
    This Record Frame is found in the Footer Envelope of an RNTuple.
    It is an extension of the "Schema Description" located in the Header Envelope.
    The schema description is not yet implemented.


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


########################################################################################################################
# Page List Envelope Frames


@serializable
class ClusterSummary(RecordFrame):
    """A class representing an RNTuple Cluster Summary Record Frame.
    The Cluster Summary Record Frame is found in the Page List Envelopes of an RNTuple.
    The Cluster Summary Record Frame contains the entry range of a cluster.
    The order of Cluster Summaries defines the cluster IDs, starting from
        the first cluster ID of the cluster group that corresponds to the page list.

    Attributes:
        firstEntryNumber (int): The first entry number in the cluster.
        nEntries (int): The number of entries in the cluster.
        featureFlag (int): The feature flag for the cluster.
    """

    # Notes:
    # Flag 0x01 is reserved for a future specification version that will support sharded clusters.
    # The future use of sharded clusters will break forward compatibility and thus introduce a corresponding feature flag.
    # For now, readers should abort when this flag is set. Other flags should be ignored.

    fFirstEntryNumber: Annotated[int, Fmt("<Q")]
    fNEntriesAndFeatureFlag: Annotated[int, Fmt("<Q")]
    #### Note: nEntries and featureFlag are encoded together in a single 64 bit integer
    # nEntries: int  # The 56 least significant bits of the 64 bit integer
    # featureFlag: int  # The 8 most significant bits of the 64 bit integer

    @property
    def fNEntries(self) -> int:
        """The number of entries in the cluster."""
        # The 56 least significant bits of the 64 bit integer
        return self.fNEntriesAndFeatureFlag & 0x00FFFFFFFFFFFFFF

    @property
    def fFeatureFlag(self) -> int:
        """The feature flag for the cluster."""
        # The 8 most significant bits of the 64 bit integer
        return (self.fNEntriesAndFeatureFlag >> 56) & 0x00000000000000FF


@serializable
class RPageDescription(ROOTSerializable):
    """A class representing an RNTuple Page Description.
    This class represents the location of a page for a column for a cluster.

    Attributes:
        fNElements (int): The number of elements in the page.
        locator (RLocator): The locator for the page.

    Notes:
    This class is the Inner Item in the triple nested List Frame of RNTuple page locations.

    [top-most[outer[inner[*Page Description*]]]]:

        Top-Most List Frame -> Outer List Frame -> Inner List Frame ->  Inner Item
            Clusters     ->      Columns     ->       Pages      ->  Page Description

    Note that Page Description is not a record frame.
    """

    fNElements: Annotated[int, Fmt("<i")]
    locator: RLocator

    def get_page(self, fetch_data: DataFetcher) -> RPage:
        """Reads the page data from the data source using the locator.
        Pages are wrapped in compression blocks (like envelopes).
        """

        #### Load the (possibly compressed) Page into the buffer
        buffer = self.locator.get_buffer(fetch_data)

        #### Read the page from the buffer
        page, buffer = RPage.read(buffer)

        # TODO: compression
        # check buffer is empty?
        return page


@serializable
class PageLocations_Pages(ListFrame[RPageDescription]):
    """A class representing the RNTuple Page Locations Pages (Inner) List Frame.
    This class represents the locations of pages for a column for a cluster.
    This class is a specialized `ListFrame` that holds `PageDescription` objects,
        where each object corresponds to a page, and each object represents
        the location of that page.
    This is a unique `ListFrame`, as it stores extra column information that
        is located after the list of `PageDescription` objects.
    The order of the pages matches the order of the pages in the ROOT file.
    The element offset is negative if the column is suppressed.

    Attributes:
        elementoffset (int): The offset for the first element for this column.
        compressionsettings (int | None): The compression settings for the pages in this column.

    Notes:
    This class is the Inner List Frame in the triple nested List Frame of RNTuple page locations.

    [top-most[outer[*inner*[Page Description]]]]:

        Top-Most List Frame -> Outer List Frame -> Inner List Frame ->  Inner Item
            Clusters     ->      Columns     ->       Pages      ->  Page Description

    Note that Page Description is not a record frame.
    """

    elementoffset: int
    compressionsettings: int | None

    @classmethod
    def read(cls, buffer: ReadBuffer) -> tuple[PageLocations_Pages, ReadBuffer]:
        """Reads the Page List Frame of Page Locations from the buffer."""
        # Read the Page List as a ListFrame
        pagelist, buffer = cls.read_as(RPageDescription, buffer)
        return pagelist, buffer

    @classmethod
    def read_members(cls, buffer: ReadBuffer) -> tuple[tuple[Any, ...], ReadBuffer]:
        """Reads the extra members of the Page List Frame from the buffer."""
        # Read the element offset for this column
        (elementoffset,), buffer = buffer.unpack("<q")

        compressionsettings = None
        if elementoffset >= 0:  # If the column is not suppressed
            # Read the compression settings
            (compressionsettings,), buffer = buffer.unpack("<I")

        return (elementoffset, compressionsettings), buffer


@serializable
class PageLocations_Columns(ListFrame[PageLocations_Pages]):
    """A class representing the RNTuple Page Locations Column (Outer) List Frame.
    This class represents the locations of pages within each column for a cluster.
    This class is a specialized `ListFrame` that holds `PageLocations_Pages` objects,
        where each object corresponds to a column, and each object represents
        the locations of pages for that column.
    The order of the columns matches the order of the columns in the schema description
        and schema description extension (small to large).
    This List Frame is found in the Page List Envelope of an RNTuple.

    Notes:
    This class is the Outer List Frame in the triple nested List Frame of RNTuple page locations.

    [top-most[*outer*[inner[Page Description]]]]:

        Top-Most List Frame -> Outer List Frame -> Inner List Frame ->  Inner Item
            Clusters     ->      Columns     ->       Pages      ->  Page Description

    Note that Page Description is not a record frame.
    """

    @classmethod
    def read(cls, buffer: ReadBuffer) -> tuple[PageLocations_Columns, ReadBuffer]:
        """Reads the Column List Frame of Page Locations from the buffer."""
        # Read the Column List as a ListFrame
        columnlist, buffer = cls.read_as(PageLocations_Pages, buffer)
        return columnlist, buffer


@serializable
class PageLocations_Clusters(ListFrame[PageLocations_Columns]):
    """A class representing the RNTuple Page Locations Cluster (Top-Most) List Frame.
    This class represents the locations of pages within columns for each cluster.
    This class is a specialized `ListFrame` that holds `PageLocations_Columns` objects,
        where each object corresponds to a cluster, and each object represents
        the locations of pages for each column in that cluster.
    The order of the clusters corresponds to the cluster IDs as defined
        by the cluster groups and cluster summaries.
    This List Frame is found in the Page List Envelope of an RNTuple.

    Notes:
    This class is the Top-Most List Frame in the triple nested List Frame of RNTuple page locations.

    [*top-most*[outer[inner[Page Description]]]]:

        Top-Most List Frame -> Outer List Frame -> Inner List Frame ->  Inner Item
            Clusters     ->      Columns     ->       Pages      ->  Page Description

    Note that Page Description is not a record frame.
    """

    @classmethod
    def read(cls, buffer: ReadBuffer) -> tuple[PageLocations_Clusters, ReadBuffer]:
        """Reads the Cluster List Frame of Page Locations from the buffer."""
        # Read the Cluster List as a ListFrame
        clusterlist, buffer = cls.read_as(PageLocations_Columns, buffer)
        return clusterlist, buffer

    def find_page(self, column_index: int, entry: int) -> RPageDescription | None:
        # TODO: test method
        for cluster in self:
            column = cluster[column_index]
            if column.elementoffset <= entry:
                cluster_local_offset = entry - column.elementoffset
                offset = 0
                for page in column:
                    offset += page.fNElements
                    if offset > cluster_local_offset:
                        return page  # type: ignore[no-any-return]
        return None
