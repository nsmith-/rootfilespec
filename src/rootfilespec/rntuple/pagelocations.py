from dataclasses import dataclass
from typing import Annotated

import xxhash  # type: ignore[import-not-found]

from rootfilespec.bootstrap.compression import RCompressionSettings
from rootfilespec.rntuple.RFrame import Item, ListFrame
from rootfilespec.rntuple.RLocator import FileLocator, RLocator, in_file
from rootfilespec.rntuple.RPage import RPage
from rootfilespec.serializable import (
    ReadBuffer,
    ROOTSerializable,
    serializable,
)
from rootfilespec.structutil import Fmt, OptionalField

PAGE_CHECKSUM_SIZE = 8
"""A page checksum is an XXH3-64, stored little-endian right after the page"""


@serializable
class RPageDescription(ROOTSerializable):
    """A class representing an RNTuple Page Description.
    This class represents the location of a page for a column for a cluster,
    as stored. Fetch the page through its ``page_locator``.

    Notes:
    This class is the Inner Item in the triple nested List Frame of RNTuple page locations.

    [top-most[outer[inner[*Page Description*]]]]:

        Top-Most List Frame -> Outer List Frame -> Inner List Frame ->  Inner Item
            Clusters     ->      Columns     ->       Pages      ->  Page Description
    Note that Page Description is not a record frame.
    """

    fNElements: Annotated[int, Fmt("<i")]
    """The number of elements in the page, as stored.

    Its sign is the checksum flag: negative means an XXH3-64 checksum follows the
    page. Kept as stored so that the bytes can be reproduced; use ``n_elements``
    and ``has_checksum``."""
    locator: RLocator
    """The locator for the page."""

    @property
    def n_elements(self) -> int:
        """The number of elements in the page."""
        return abs(self.fNElements)

    @property
    def has_checksum(self) -> bool:
        """Whether an XXH3-64 checksum is stored right after the page."""
        return self.fNElements < 0

    @property
    def page_locator(self) -> "RPageLocator":
        """A locator for the page, and its checksum if it has one.

        Raises NotImplementedError if the page's locator is not in the file."""
        return RPageLocator(in_file(self.locator, "Page"), self.has_checksum)


@dataclass(frozen=True)
class RPageLocator:
    """A locator for an RNTuple page.

    This follows the locator pattern, like REnvelopeLocator for envelopes: it
    describes where a page is and how to read it, and the caller fetches it.
    It covers the page as stored and, if the page has one, its checksum.
    """

    locator: FileLocator
    """The page's locator, as stored in its page description."""
    has_checksum: bool
    """Whether an XXH3-64 checksum is stored right after the page."""

    @property
    def offset(self) -> int:
        """The byte offset of the page in the file."""
        return self.locator.offset

    @property
    def size(self) -> int:
        """The number of bytes to fetch: the page as stored, then its checksum if it has one.

        The spec's page size, which excludes the checksum, is ``locator.size``."""
        return self.locator.size + (PAGE_CHECKSUM_SIZE if self.has_checksum else 0)

    def read_from(self, buffer: ReadBuffer) -> RPage:
        """Read the page from the given buffer, and verify its checksum if it has one.

        The buffer holds ``size`` bytes. The checksum covers the page as stored
        (sealed, possibly compressed), wherever those bytes were fetched from.
        Pages are wrapped in compression blocks (like envelopes); nothing is
        decompressed here.
        """
        if len(buffer) != self.size:
            msg = (
                f"RPageLocator.read_from: expected {self.size} bytes, got {len(buffer)}"
            )
            raise ValueError(msg)

        #### Read the page from the buffer
        if not self.has_checksum:
            page, _ = RPage.read(buffer)
            return page

        data, buffer = buffer.consume(self.locator.size)
        (checksum,), buffer = buffer.unpack("<Q")
        computed = xxhash.xxh3_64_intdigest(data)
        if computed != checksum:
            msg = (
                f"Page checksum mismatch at {self.locator}: "
                f"stored {checksum:#018x}, computed {computed:#018x}"
            )
            raise ValueError(msg)
        return RPage(data, checksum)


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
