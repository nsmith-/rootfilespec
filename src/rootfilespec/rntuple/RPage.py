from dataclasses import dataclass

from rootfilespec.serializable import ReadBuffer, ROOTSerializable


@dataclass
class RPage(ROOTSerializable):
    """A class to represent an RNTuple page."""

    page: bytes
    """The RNTuple page raw data, as stored (still compressed if the page is)."""
    checksum: int | None = None
    """The page's XXH3-64 checksum, verified against ``page``, or None if it has none."""

    # TODO: Flush out RPage class
    @classmethod
    def read(cls, buffer: ReadBuffer) -> tuple["RPage", ReadBuffer]:
        """Reads an RPage from the buffer."""

        # For now, just return the entire buffer
        page, buffer = buffer.consume(len(buffer))

        return cls(page), buffer
