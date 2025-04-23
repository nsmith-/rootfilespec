from __future__ import annotations

from typing import Annotated, Any

from rootfilespec.bootstrap.RFrame import ListFrame
from rootfilespec.bootstrap.RLocator import RLocator
from rootfilespec.bootstrap.RPage import RPage
from rootfilespec.structutil import (
    DataFetcher,
    Fmt,
    ReadBuffer,
    ROOTSerializable,
    serializable,
)


@serializable
class RPageDescription(ROOTSerializable):
    """A class representing an RNTuple Page Description.
    This class represents the location of a page for a column for a cluster.

    Notes:
    This class is the Inner Item in the triple nested List Frame of RNTuple page locations.

    [top-most[outer[inner[*Page Description*]]]]:

        Top-Most List Frame -> Outer List Frame -> Inner List Frame ->  Inner Item
            Clusters     ->      Columns     ->       Pages      ->  Page Description
    Note that Page Description is not a record frame.
    """

    fNElements: Annotated[int, Fmt("<i")]
    """The number of elements in the page."""
    locator: RLocator
    """The locator for the page."""

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
class PageLocations(ListFrame[RPageDescription]):
    """A class representing the RNTuple Page Locations Pages (Inner) List Frame.
    This class represents the locations of pages for a column for a cluster.
    This class is a specialized `ListFrame` that holds `RPageDescription` objects,
        where each object corresponds to a page, and each object represents
        the location of that page.
    This is a unique `ListFrame`, as it stores extra column information that
        is located after the list of `RPageDescription` objects.
    The order of the pages matches the order of the pages in the ROOT file.
    The element offset is negative if the column is suppressed.

    Notes:
    This class is the Inner List Frame in the triple nested List Frame of RNTuple page locations.

    [top-most[outer[*inner*[Page Description]]]]:

        Top-Most List Frame -> Outer List Frame -> Inner List Frame ->  Inner Item
            Clusters     ->      Columns     ->       Pages      ->  Page Description

    Note that Page Description is not a record frame.
    """

    elementoffset: int
    """The offset for the first element for this column."""
    compressionsettings: int | None
    """The compression settings for the pages in this column."""

    @classmethod
    def read(cls, buffer: ReadBuffer) -> tuple[PageLocations, ReadBuffer]:
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
class ColumnLocations(ListFrame[PageLocations]):
    """A class representing the RNTuple Page Locations Column (Outer) List Frame.
    This class represents the locations of pages within each column for a cluster.
    This class is a specialized `ListFrame` that holds `PageLocations` objects,
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
    def read(cls, buffer: ReadBuffer) -> tuple[ColumnLocations, ReadBuffer]:
        """Reads the Column List Frame of Page Locations from the buffer."""
        # Read the Column List as a ListFrame
        columnlist, buffer = cls.read_as(PageLocations, buffer)
        return columnlist, buffer


@serializable
class ClusterLocations(ListFrame[ColumnLocations]):
    """A class representing the RNTuple Page Locations Cluster (Top-Most) List Frame.
    This class represents the locations of pages within columns for each cluster.
    This class is a specialized `ListFrame` that holds `ColumnLocations` objects,
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
    def read(cls, buffer: ReadBuffer) -> tuple[ClusterLocations, ReadBuffer]:
        """Reads the Cluster List Frame of Page Locations from the buffer."""
        # Read the Cluster List as a ListFrame
        clusterlist, buffer = cls.read_as(ColumnLocations, buffer)
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
