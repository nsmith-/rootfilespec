from __future__ import annotations

from dataclasses import dataclass

from rootfilespec.structutil import ReadBuffer, ROOTSerializable


@dataclass
class RPage(ROOTSerializable):
    """A class to represent an RNTuple page.

    Attributes:
        page (bytes): The page raw data.
    """
    page: bytes

    # TODO: Flush out RPage class
    @classmethod
    def read(cls, buffer: ReadBuffer):
        """Reads an RPage from the buffer."""
        
        # For now, just return the entire buffer
        page, buffer = buffer.consume(len(buffer))

        return cls(page), buffer