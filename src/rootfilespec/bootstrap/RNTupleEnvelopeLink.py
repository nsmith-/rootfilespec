from __future__ import annotations

from dataclasses import dataclass

from ..structutil import (
    DataFetcher,
    ReadBuffer,
    ROOTSerializable,
    # StructClass,
    # sfield,
    # structify,
)

# Use map to avoid circular imports
DICTIONARY_ENVELOPE: dict[bytes, type[ROOTSerializable]] = {}


@dataclass
class RNTupleEnvelopeLink(ROOTSerializable):
    """A class representing the RNTuple Envelope Link (somewhat analogous to a TKey).
    An Envelope Link references an Envelope in an RNTuple.
    An Envelope Link is consists of a 64 bit unsigned integer that specifies the
        uncompressed size (i.e. length) of the envelope, followed by a Locator.

    Envelope Links of this form (currently seem to be) only used to locate Page List Envelopes.
    The Header Envelope and Footer Envelope are located using the information in the RNTuple Anchor.
    The Header and Footer Envelope Links are created in the RNTuple Anchor class by casting directly to this class.

    Attributes:
        length (int): Uncompressed size of the envelope
        locator (RNTupleLocator): Locator for the envelope
    """

    length: int  # Uncompressed size of the envelope
    locator: RNTupleLocator  # Locator base class

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """Reads a RNTuple envelope link from the given buffer."""

        # All envelope links start with the envelope length
        (length,), buffer = buffer.unpack("<Q")

        # Read the locator
        locator, buffer = RNTupleLocator.read(buffer)

        return cls(length, locator), buffer

    def get_buffer(self, fetch_data: DataFetcher):
        """Returns the buffer for the byte range specified by the locator."""
        return self.locator.get_buffer(fetch_data)

    def is_compressed(self) -> bool:
        """Returns whether the envelope is compressed.
        e.g. if the (compressed) size of the envelope is different from the (uncompressed) length of the envelope.
        """

        # Check each type of locator for compression
        if self.locator.isStandard:  # Standard locator
            return self.locator.locatorSubclass.size != self.length
        if self.locator.locatorSubclass.locatorType == 0x01:  # Large locator
            return self.locator.locatorSubclass.payload.size != self.length
        msg = f"Unknown locator type: {self.locator.locatorSubclass.locatorType=}"
        raise NotImplementedError(msg)

    def read_envelope(self, fetch_data: DataFetcher) -> ROOTSerializable:
        """Reads an RNTupleEnvelope from the given buffer."""
        # Load the (possibly compressed) envelope into the buffer
        # This should load exactly the envelope bytes (no more, no less)
        buffer = self.locator.get_buffer(fetch_data)

        # If compressed, decompress the envelope
        # compressed = None
        compressed = self.is_compressed()
        # if self.size != self.length:
        if self.is_compressed():
            msg = f"Compressed envelopes are not yet supported: {self.locator.locatorSubclass.locatorType=}"
            raise NotImplementedError(msg)
            # TODO: Implement compressed envelopes

        # Now the envelope is uncompressed

        # envelope, buffer = RNTupleEnvelope.read(buffer)
        envelope, buffer = DICTIONARY_ENVELOPE[b"RNTupleEnvelope"].read(buffer)

        # check that buffer is empty
        if buffer:
            msg = "RNTupleEnvelopeLink.read_envelope: buffer not empty after reading envelope."
            msg += f"\n{self=}"
            msg += f"\n{compressed=}"
            msg += f"\n{envelope=}"
            msg += f"\nBuffer: {buffer}"
            raise ValueError(msg)

        return envelope


@dataclass
class RNTupleLocator(ROOTSerializable):
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
        isStandard (bool): Whether the locator is a standard locator.
        locatorSubclass (RNTupleStandardLocator | RNTupleNonStandardLocator): The subclass of the locator.

    Methods:
        read(buffer: ReadBuffer) -> RNTupleLocator: Reads a RNTuple locator from the given buffer.
        get_buffer(fetch_data: DataFetcher) -> ReadBuffer: Returns the buffer for the byte range specified by the locator.
    """

    isStandard: bool
    locatorSubclass: RNTupleStandardLocator | RNTupleNonStandardLocator

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """Reads a RNTuple locator from the given buffer."""
        # Peek (don't update buffer) at the first 32 bit integer in the buffer to determine the locator type
        (sizeType,), _ = buffer.unpack("<i")

        isStandard = sizeType >= 0  # Standard Locator if sizeType is positive

        if isStandard:
            locatorSubclass, buffer = RNTupleStandardLocator.read(buffer)
        else:
            locatorSubclass, buffer = RNTupleNonStandardLocator.read(buffer)
        return cls(isStandard, locatorSubclass), buffer

    def get_buffer(self, fetch_data: DataFetcher):
        """Returns the buffer for the byte range specified by the locator."""
        return self.locatorSubclass.get_buffer(fetch_data)


@dataclass
class RNTupleStandardLocator(ROOTSerializable):
    """A class representing a Standard RNTuple Locator.
    A locator is a generalized way to specify a certain byte range on the storage medium.
    A standard locator is a locator that specifies a byte size and byte offset. (simple on-disk or in-file locator).

    Attributes:
        size (int): The size of the byte range.
        offset (int): The byte offset to the byte range.

    Methods:
        read(buffer: ReadBuffer) -> RNTupleStandardLocator: Reads a standard RNTuple locator from the given buffer.
        get_buffer(fetch_data: DataFetcher) -> ReadBuffer: Returns the buffer for the byte range specified by the standard locator.
    """

    size: int
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
class RNTupleNonStandardLocator(ROOTSerializable):
    """A base class representing a Non-Standard RNTuple Locator.
    A non-standard locator can specify additional information beyond just the size and offset.
    This class is a base class for different types of non-standard locators.
    All non-standard locators have the same header format, but unique payloads.

    Attributes:
        locatorSize (int): The (uncompressed) size of the locator itself.
        reserved (int): Reserved by ROOT developers for future use.
        locatorType (int): The type of the non-standard locator.
        payload (RNTupleLargeLocator_payload): The payload of the non-standard locator.

    Notes:
    Currently available non-standard locator types:

        Type	Meaning	        Payload format
        0x01	Large locator	64bit size followed by 64bit offset

    The range 0x02 - 0x7f is reserved for future use.

    Methods:
        read(buffer: ReadBuffer) -> RNTupleNonStandardLocator: Reads a non-standard RNTuple locator from the given buffer.
        get_buffer(fetch_data: DataFetcher) -> ReadBuffer: Returns the buffer for the byte range specified by the non-standard locator.
    """

    locatorSize: int
    reserved: int
    locatorType: int
    payload: RNTupleLargeLocator_payload

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """Reads a non-standard RNTuple locator from the given buffer."""

        # The first 32 bit signed integer contains the locator size, reserved, and locator type
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
            payload, buffer = RNTupleLargeLocator_payload.read(buffer)
        else:
            msg = f"Unknown non-standard locator type: {locatorType=}"
            raise ValueError(msg)

        return cls(locatorSize, reserved, locatorType, payload), buffer

    def get_buffer(self, fetch_data: DataFetcher):
        """Returns the buffer for the byte range specified by the non-standard locator."""
        return self.payload.get_buffer(fetch_data)


@dataclass
class RNTupleLargeLocator_payload(ROOTSerializable):
    """A class representing the payload of the "Large" type of Non-Standard RNTuple Locator .
    A Large Locator is like the standard on-disk locator but with a 64bit size instead of 32bit.
    The type for the Large Locator is 0x01 (specified in the RNTupleNonStandardLocator base class).

    Attributes:
        size (int): The (compressed) size of the byte range.
        offset (int): The byte offset to the byte range.

    Methods:
        read(buffer: ReadBuffer) -> RNTupleLargeLocator_payload: Reads a payload for a "Large" type of Non-Standard RNTuple locator from the given buffer.
        get_buffer(fetch_data: DataFetcher) -> ReadBuffer: Returns the buffer for the byte range specified by the "Large Locator" payload.
    """

    size: int
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
