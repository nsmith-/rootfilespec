from dataclasses import dataclass

from rootfilespec.serializable import ReadBuffer, ROOTSerializable


@dataclass
class RLocator(ROOTSerializable):
    """A base class representing an RNTuple locator data structure.

    An RLocator is a generalized way to specify a certain byte range on the storage medium.
    For disk-based storage, the locator contains byte offset and byte size.
    For other storage systems, the locator contains enough information to retrieve the referenced block,
    e.g. in object stores, the locator can specify a certain object ID.

    All locators begin with a signed 32 bit integer.
    If the integer is positive, the locator is a standard locator.
    For standard locators, the size of the byte range to locate is the absolute value of the integer.
    If the integer is negative, the locator is a non-standard locator.
    Size and type mean different things for standard and non-standard locators.

    This base class checks the type of the locator and reads the appropriate subclass:
    StandardLocator, LargeLocator (type 0x01), or UnknownLocator for any other
    non-standard type, as ROOT reads them (``RNTupleSerializer::DeserializeLocator``,
    ``tree/ntuple/src/RNTupleSerialize.cxx:1112-1155`` at 6.40.04).

    All Envelope Links will have an RLocator, but an RLocator doesn't require an Envelope Link.
    (See the Page Location in the Page List Envelopes for an example of an RLocator without an Envelope Link.)

    Note: RLocator itself is just a data structure.
    It is used as a building block by full locators like REnvelopeLocator and RPageLocator,
    which implement the Locator protocol with offset, size, and read_from() methods.
    Only the locators of a byte range in the file (FileLocator) can be fetched that way.
    """

    @classmethod
    def read(cls, buffer: ReadBuffer) -> tuple["RLocator", ReadBuffer]:
        """Reads a RNTuple locator from the given buffer.

        Which class a locator is depends on its first word, so locators are read
        through ``RLocator.read``; reading one as ``LargeLocator`` or
        ``UnknownLocator`` could give another class, and raises TypeError.
        """
        if cls is not RLocator:
            msg = f"Read locators through RLocator.read, not {cls.__name__}.read"
            raise TypeError(msg)

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
        # The first 32 bit signed integer is the negated (size, reserved, type)
        # word: ROOT negates the whole word, then splits it (SerializeLocator and
        # DeserializeLocator, RNTupleSerialize.cxx:1104-1107 and :1124-1127 at
        # 6.40.04). The spec's "absolute value of the 8bit integer" is that type
        # only when the low 24 bits are zero; they never are, since they hold
        # the locator's size.
        (negatedHead,), buffer = buffer.unpack("<i")
        head = -negatedHead
        # The 16 least significant bits: the size of the locator itself, this
        # word included; the payload follows the word
        locatorSize = head & 0xFFFF
        # The next 8 bits are reserved for the storage backend of the type
        reserved = (head >> 16) & 0xFF
        # The 8 most significant bits (of which the top one is the sign): the type
        locatorType = head >> 24
        payloadSize = locatorSize - 4
        if payloadSize < 0:
            msg = f"Non-standard locator of type {locatorType:#04x} has size {locatorSize}, less than its own 4-byte header"
            raise ValueError(msg)
        payload, buffer = buffer.consume(payloadSize)

        # Read the payload based on the locator type
        if locatorType == 0x01:
            return LargeLocator.from_payload(payload, reserved), buffer

        # Any other type, 0x02 (DAOS Object64, root-io-spec ERRATA 4; reading it
        # is #148) included, is kept as stored, as ROOT keeps a locator of a type
        # it doesn't know (kTypeUnknown); only fetching through it fails (#122)
        return UnknownLocator(locatorType, reserved, payload), buffer


@dataclass
class StandardLocator(RLocator):
    """A class representing a Standard RNTuple Locator.
    A locator is a generalized way to specify a certain byte range on the storage medium.
    A standard locator is a locator that specifies a byte size and byte offset. (simple on-disk or in-file locator).
    """

    size: int
    """The (compressed) size of the byte range to locate."""
    offset: int
    """The byte offset to the byte range to locate."""

    @classmethod
    def read(cls, buffer: ReadBuffer) -> tuple["StandardLocator", ReadBuffer]:
        """Reads a standard RNTuple locator from the given buffer."""

        # Size is the absolute value of a signed 32 bit integer
        (size,), buffer = buffer.unpack("<i")
        size = abs(size)

        # Offset is a 64 bit unsigned integer
        (offset,), buffer = buffer.unpack("<Q")

        return cls(size, offset), buffer


@dataclass
class LargeLocator(RLocator):
    """A class representing the payload of the "Large" type of Non-Standard RNTuple Locator .
    A Large Locator is like the standard on-disk locator but with a 64bit size instead of 32bit.
    The type for the Large Locator is 0x01.
    """

    size: int
    """The (compressed) size of the byte range to locate."""
    offset: int
    """The byte offset to the byte range to locate."""
    reserved: int = 0
    """The locator's reserved byte, as stored (ROOT writes 0)."""

    @classmethod
    def from_payload(cls, payload: bytes, reserved: int) -> "LargeLocator":
        """Reads the payload of a "Large" non-standard locator: a 64 bit size, then a 64 bit offset."""
        if len(payload) != 16:
            msg = f"Large locator payload is {len(payload)} bytes, expected 16"
            raise ValueError(msg)
        size = int.from_bytes(payload[:8], "little")
        offset = int.from_bytes(payload[8:], "little")
        return cls(size, offset, reserved)


@dataclass
class UnknownLocator(RLocator):
    """A non-standard locator of a type that rootfilespec does not read (#122)

    Kept as stored, so that the page list or footer holding it still reads, as
    in ROOT, which reads such a locator as ``kTypeUnknown``. What it locates
    can't be fetched: ``page_locator`` and ``envelope_locator`` raise on it.
    """

    locatorType: int
    """The locator type: not 0x01. 0x02 is ROOT's DAOS locator (root-io-spec
    ERRATA 4; reading it is #148), 0x7e the one ROOT's tests write."""
    reserved: int
    """The locator's reserved byte, as stored."""
    payload: bytes
    """The payload, as stored: the locator's size, less its 4-byte header."""


FileLocator = StandardLocator | LargeLocator
"""The locators of a byte range in the file: an offset and a size"""


def in_file(locator: RLocator, what: str) -> FileLocator:
    """The locator, if it locates a byte range in the file

    Only those can be fetched through the Locator protocol, which is an offset
    and a size in the file. ``what`` names the referenced block for the error.
    """
    if isinstance(locator, StandardLocator | LargeLocator):
        return locator
    msg = f"{what} has a locator that is not in the file, which can't be fetched: {locator}"
    raise NotImplementedError(msg)
