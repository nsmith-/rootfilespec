from dataclasses import dataclass

from rootfilespec.buffer import ReadBuffer
from rootfilespec.serializable import ROOTSerializable


@dataclass
class RPage(ROOTSerializable):
    """A class to represent an RNTuple page."""

    page: bytes
    """The RNTuple page raw data."""

    # TODO: Flush out RPage class
    @classmethod
    def read(cls, buffer: ReadBuffer) -> tuple["RPage", ReadBuffer]:
        """Reads an RPage from the buffer."""

        # For now, just return the entire buffer
        page, buffer = buffer.consume(len(buffer))

        return cls(page), buffer
