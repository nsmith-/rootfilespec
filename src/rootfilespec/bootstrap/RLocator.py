from __future__ import annotations

from dataclasses import dataclass

from rootfilespec.structutil import (
    DataFetcher,
    ReadBuffer,
    ROOTSerializable,
)


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
    """

    size: int
    """The (compressed) size of the byte range to locate."""

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

        # # The locator size is the 16 least significant bits
        # locatorSize = locatorSizeReservedType & 0xFFFF  # Size of locator itself

        # # The reserved field is the next 8 least significant bits
        # reserved = (
        #     locatorSizeReservedType >> 16
        # ) & 0xFF  # Reserved by ROOT developers for future use

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
    """

    offset: int
    """The byte offset to the byte range to locate."""

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
    """

    offset: int
    """The byte offset to the byte range to locate."""

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
