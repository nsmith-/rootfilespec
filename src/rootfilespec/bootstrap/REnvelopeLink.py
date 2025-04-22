from __future__ import annotations

from dataclasses import dataclass
from typing import TypeVar

from rootfilespec.bootstrap.RLocator import RLocator
from rootfilespec.structutil import (
    DataFetcher,
    ReadBuffer,
    ROOTSerializable,
)

EnvType = TypeVar("EnvType", bound=ROOTSerializable)


@dataclass
class REnvelopeLink(ROOTSerializable):
    """A class representing the RNTuple Envelope Link (somewhat analogous to a TKey).
    An Envelope Link references an Envelope in an RNTuple.
    An Envelope Link is consists of a 64 bit unsigned integer that specifies the
        uncompressed size (i.e. length) of the envelope, followed by a Locator.

    Envelope Links of this form (currently seem to be) only used to locate Page List Envelopes.
    The Header Envelope and Footer Envelope are located using the information in the RNTuple Anchor.
    The Header and Footer Envelope Links are created in the RNTuple Anchor class by casting directly to this class.

    Attributes:
        length (int): Uncompressed size of the envelope
        locator (RLocator): Locator for the envelope
    """

    length: int  # Uncompressed size of the envelope
    locator: RLocator  # Locator base class

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """Reads a RNTuple Envelope Link from the given buffer."""

        # All envelope links start with the envelope length
        (length,), buffer = buffer.unpack("<Q")

        # Read the locator
        locator, buffer = RLocator.read(buffer)

        return cls(length, locator), buffer

    def get_buffer(self, fetch_data: DataFetcher):
        """Returns the buffer for the byte range specified by the locator."""
        return self.locator.get_buffer(fetch_data)

    def read_envelope(
        self,
        fetch_data: DataFetcher,
        envtype: type[EnvType],
    ) -> EnvType:
        """Reads an REnvelope from the given buffer."""
        # Load the (possibly compressed) envelope into the buffer
        # This should load exactly the envelope bytes (no more, no less)
        # buffer = fetch_data(self.offset, self.size)

        buffer = self.locator.get_buffer(fetch_data)

        # If compressed, decompress the envelope
        # compressed = None

        if self.locator.size != self.length:
            msg = f"Compressed envelopes are not yet supported: {self.locator}"
            raise NotImplementedError(msg)
            # TODO: Implement compressed envelopes

        # Now the envelope is uncompressed

        #### Peek at the metadata to determine the type of envelope but don't consume it
        (lengthType,), _ = buffer.unpack("<Q")

        # Envelope type, encoded in the 16 least significant bits
        typeID = lengthType & 0xFFFF
        # Envelope size (uncompressed), encoded in the 48 most significant bits
        length = lengthType >> 16
        # Ensure that the length of the envelope matches the buffer length
        if length != len(buffer):
            msg = f"Length of envelope ({length}) of type {typeID} does not match buffer length ({len(buffer)})"
            raise ValueError(msg)

        #### Read the envelope
        envelope, buffer = envtype.read(buffer)

        # check that buffer is empty
        if buffer:
            msg = (
                "REnvelopeLink.read_envelope: buffer not empty after reading envelope."
            )
            msg += f"\n{self=}"
            msg += f"\n{envelope=}"
            msg += f"\nBuffer: {buffer}"
            raise ValueError(msg)

        return envelope
