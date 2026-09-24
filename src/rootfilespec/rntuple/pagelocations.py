from collections.abc import Callable
from dataclasses import dataclass
from typing import Annotated, cast

import xxhash  # type: ignore[import-not-found]

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

PAGE_CHECKSUM_SIZE = 8
"""A page checksum is an XXH3-64, stored little-endian right after the page"""


@dataclass(frozen=True)
class RPageLocator:
    """The locator of a page's stored bytes: the page and, if it has one, its checksum

    ``size`` is the stored size, so fetching ``(offset, size)`` gets everything
    ``read_from`` needs. ``read_from`` verifies the checksum over the stored
    (sealed, possibly compressed) bytes and does not decompress the page.
    """

    offset: int
    """The byte offset of the page in the file."""
    size: int
    """The stored size: the page, plus the checksum if it has one."""
    has_checksum: bool
    """Whether an XXH3-64 checksum follows the page."""

    def read_from(self, buffer: ReadBuffer) -> RPage:
        if len(buffer) != self.size:
            msg = (
                f"RPageLocator.read_from: expected {self.size} bytes, got {len(buffer)}"
            )
            raise ValueError(msg)
        if not self.has_checksum:
            page, _ = RPage.read(buffer)
            return page
        data, buffer = buffer.consume(self.size - PAGE_CHECKSUM_SIZE)
        (checksum,), buffer = buffer.unpack("<Q")
        computed = xxhash.xxh3_64_intdigest(data)
        if computed != checksum:
            msg = (
                f"Page checksum mismatch at offset {self.offset}: "
                f"stored {checksum:#018x}, computed {computed:#018x}"
            )
            raise ValueError(msg)
        return RPage(data, checksum)


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
    """The number of elements in the page, as stored.

    Its sign is the checksum flag: negative means an XXH3-64 checksum follows the
    page. Kept as stored so that the bytes can be reproduced; use ``n_elements``
    and ``has_checksum``."""
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
        """The (compressed) size of the page data, as in the locator.

        As the spec says, it does not include the checksum; see ``stored_size``."""
        return self.locator.size

    @property
    def n_elements(self) -> int:
        """The number of elements in the page."""
        return abs(self.fNElements)

    @property
    def has_checksum(self) -> bool:
        """Whether an XXH3-64 checksum is stored right after the page."""
        return self.fNElements < 0

    @property
    def stored_size(self) -> int:
        """The number of bytes the page takes in the file, checksum included."""
        return self.size + (PAGE_CHECKSUM_SIZE if self.has_checksum else 0)

    @property
    def page_locator(self) -> RPageLocator:
        """A locator for the page's stored bytes, which verifies the checksum."""
        return RPageLocator(self.offset, self.stored_size, self.has_checksum)

    def read_from(self, buffer: ReadBuffer) -> RPage:
        """Read the page from the given buffer, without its checksum.

        Pages are wrapped in compression blocks (like envelopes). Prefer
        ``page_locator``, which also reads and verifies the checksum.
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
        """Reads the page, and verifies its checksum, using ``page_locator``.
        Pages are wrapped in compression blocks (like envelopes).
        """
        loc = self.page_locator
        return loc.read_from(fetch_data(loc))


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
