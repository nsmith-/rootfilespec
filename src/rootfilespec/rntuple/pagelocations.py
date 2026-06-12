from collections.abc import Callable
from typing import Annotated, cast

from rootfilespec.bootstrap.compression import RCompressionSettings
from rootfilespec.rntuple.RFrame import Item, ListFrame
from rootfilespec.rntuple.RLocator import RLocator
from rootfilespec.rntuple.RPage import RPage
from rootfilespec.serializable import (
    Locator,
    ReadBuffer,
    ROOTSerializable,
    serializable,
)
from rootfilespec.structutil import Fmt, OptionalField


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

    @property
    def offset(self) -> int:
        """The byte offset of the page in the file."""
        # Note: self.locator is always StandardLocator or LargeLocator at runtime,
        # which have offset fields. Cast needed because base RLocator doesn't have offset.
        return cast(Locator[ROOTSerializable], self.locator).offset

    @property
    def size(self) -> int:
        """The (compressed) size of the page data."""
        return self.locator.size

    def read_from(self, buffer: ReadBuffer) -> RPage:
        """Read the page from the given buffer.

        Pages are wrapped in compression blocks (like envelopes).
        """
        #### Read the page from the buffer
        page, buffer = RPage.read(buffer)

        if buffer:
            msg = "RPageDescription.read_from: buffer not empty after reading page."
            raise ValueError(msg)

        return page

    def get_page(
        self, fetch_data: Callable[[Locator[ROOTSerializable]], ReadBuffer]
    ) -> RPage:
        """Reads the page data from the data source using the locator.
        Pages are wrapped in compression blocks (like envelopes).
        """
        buffer = fetch_data(self)
        return self.read_from(buffer)


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

    elementoffset: Annotated[int, Fmt("<q")]
    """The offset for the first element for this column."""
    compressionsettings: Annotated[
        RCompressionSettings | None, OptionalField("class", "elementoffset", ">=", 0)
    ]
    """The compression settings for the pages in this column."""
