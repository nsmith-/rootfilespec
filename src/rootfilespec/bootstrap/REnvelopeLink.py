from __future__ import annotations

from dataclasses import dataclass

from rootfilespec.structutil import (
    DataFetcher,
    ReadBuffer,
    ROOTSerializable,
)

# Use map to avoid circular imports
DICTIONARY_ENVELOPE: dict[bytes, type[ROOTSerializable]] = {}


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

    def read_envelope(self, fetch_data: DataFetcher) -> ROOTSerializable:
        """Reads an REnvelope from the given buffer."""
        # Load the (possibly compressed) envelope into the buffer
        # This should load exactly the envelope bytes (no more, no less)
        # buffer = fetch_data(self.offset, self.size)

        buffer = self.locator.get_buffer(fetch_data)

        # If compressed, decompress the envelope
        # compressed = None

        if self.locator.size != self.length:
            msg = f"Compressed envelopes are not yet supported: {self.locator.locatorSubclass.locatorType=}"
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

        #### Read the envelope based on the typeID
        if typeID in DICTIONARY_ENVELOPE:
            envelope, buffer = DICTIONARY_ENVELOPE[typeID].read(buffer)
        else:
            msg = f"Unknown envelope type: {typeID}"
            raise ValueError(msg)

        # check that buffer is empty
        if buffer:
            msg = "REnvelopeLink.read_envelope: buffer not empty after reading envelope."
            msg += f"\n{self=}"
            msg += f"\n{envelope=}"
            msg += f"\nBuffer: {buffer}"
            raise ValueError(msg)

        return envelope

@dataclass
class RLocator(ROOTSerializable):
    """A base class representing a Locator for RNTuples.
    A locator is a generalized way to specify a certain byte range on the storage medium.
        For disk-based storage, the locator is just byte offset and byte size.
        For other storage systems, the locator contains enough information to retrieve the referenced block,
            e.g. in object stores, the locator can specify a certain object ID.

    All locators begin with a signed 32 bit integer.
    If the integer is positive, the locator is a standard locator.
    For standard locators, the size of the byte range to locate is the absolute value of the integer.
    If the integer is negative, the locator is a non-standard locator.
    Size and type mean different things for standard and non-standard locators.

    This base class checks the type of the locator and reads the appropriate subclass.
    It contains a `get_buffer()` method that should be implemented by subclasses to return the buffer for the envelope.
    This provides forward compatibility for different locator types, as the envelope can be read using the same method.

    All Envelope Links will have a Locator, but a Locator doesn't require an Envelope Link.
    (See the Page Location in the Page List Envelopes for an example of a Locator without an Envelope Link.)

    Attributes:
        size (int): The (compressed) size of the byte range to locate.
    """

    size: int

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """Reads a RNTuple locator from the given buffer."""
        
        #### Peek (don't update buffer) at the first 32 bit integer in the buffer to determine the locator type
        # We don't want to consume the buffer yet, because RLocator_Standard will need to consume it
        (sizeType,), _ = buffer.unpack("<i")

        #### Standard locator if sizeType is positive
        # For standard locators, the first 32 bit signed integer is the size of the byte range to locate
        #   (sign indicates standard or non-standard)
        # Thus the derived class StandardLocator will need to consume it
        if sizeType >= 0:
            return StandardLocator.read(buffer)
        
        #### Non-standard locator
        # The first 32 bit signed integer contains the locator size, reserved, and locator type
        # For non-standard locators, the first 32 bits contain metadata about the locator itself
        #    (i.e. it doesn't contain any info about the byte range to locate)
        # Thus any derived classes will not need to consume it
        (locatorSizeReservedType,), buffer = buffer.unpack("<i")

        # The locator size is the 16 least significant bits
        locatorSize = locatorSizeReservedType & 0xFFFF  # Size of locator itself

        # The reserved field is the next 8 least significant bits
        reserved = (
            locatorSizeReservedType >> 16
        ) & 0xFF  # Reserved by ROOT developers for future use

        # The locator type is the 8 most significant bits (the final 8 bits)
        locatorType = (
            locatorSizeReservedType >> 24
        ) & 0xFF  # Type of non-standard locator

        # Read the payload based on the locator type
        if locatorType == 0x01:
            return LargeLocator.read(buffer)

        msg = f"Unknown non-standard locator type: {locatorType=}"
        raise ValueError(msg)

    def get_buffer(self, fetch_data: DataFetcher):
        """This should be overridden by subclasses"""
        msg = "get_buffer() not implemented for this locator type"
        raise NotImplementedError(msg)

@dataclass
class StandardLocator(RLocator):
    """A class representing a Standard RNTuple Locator.
    A locator is a generalized way to specify a certain byte range on the storage medium.
    A standard locator is a locator that specifies a byte size and byte offset. (simple on-disk or in-file locator).

    Attributes:
        offset (int): The byte offset to the byte range to locate.
    """

    offset: int

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """Reads a standard RNTuple locator from the given buffer."""

        # Size is the absolute value of a signed 32 bit integer
        (size,), buffer = buffer.unpack("<i")
        size = abs(size)

        # Offset is a 64 bit unsigned integer
        (offset,), buffer = buffer.unpack("<Q")

        return cls(size, offset), buffer

    def get_buffer(self, fetch_data: DataFetcher):
        """Returns the buffer for the byte range specified by the standard locator."""
        return fetch_data(self.offset, self.size)

@dataclass
class LargeLocator(RLocator):
    """A class representing the payload of the "Large" type of Non-Standard RNTuple Locator .
    A Large Locator is like the standard on-disk locator but with a 64bit size instead of 32bit.
    The type for the Large Locator is 0x01.

    Attributes:
        offset (int): The byte offset to the byte range to locate.
    """

    offset: int

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """Reads a payload for a "Large" type of Non-Standard RNTuple locator from the given buffer."""

        # Size is a 64 bit unsigned integer
        (size,), buffer = buffer.unpack("<Q")

        # Offset is a 64 bit unsigned integer
        (offset,), buffer = buffer.unpack("<Q")

        return cls(size, offset), buffer

    def get_buffer(self, fetch_data: DataFetcher):
        """Returns the buffer for the byte range specified by the "Large Locator" payload."""
        return fetch_data(self.offset, self.size)
