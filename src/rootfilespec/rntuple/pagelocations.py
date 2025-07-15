from __future__ import annotations

from typing import Annotated, Optional

from rootfilespec.buffer import DataFetcher, ReadBuffer
from rootfilespec.rntuple.RFrame import Item, ListFrame
from rootfilespec.rntuple.RLocator import RLocator
from rootfilespec.rntuple.RPage import RPage
from rootfilespec.serializable import Members, ROOTSerializable, serializable
from rootfilespec.structutil import Fmt


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
class PageLocations(ListFrame[Item]):
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
    compressionsettings: Optional[int]
    """The compression settings for the pages in this column."""

    @classmethod
    def update_members(
        cls, members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        """Reads the extra members of the Page List Frame from the buffer."""
        # Read the element offset for this column
        (elementoffset,), buffer = buffer.unpack("<q")

        compressionsettings = None
        if elementoffset >= 0:  # If the column is not suppressed
            # Read the compression settings
            (compressionsettings,), buffer = buffer.unpack("<I")

        members["elementoffset"] = elementoffset
        members["compressionsettings"] = compressionsettings
        return members, buffer
