from dataclasses import dataclass, field
from typing import Annotated, TypeVar

from typing_extensions import Self

from rootfilespec.buffer import DataFetcher, ReadBuffer
from rootfilespec.rntuple.RLocator import RLocator
from rootfilespec.serializable import Members, ROOTSerializable, serializable
from rootfilespec.structutil import Fmt

# Map of envelope type to string for printing
ENVELOPE_TYPE_MAP = {0x00: "Reserved"}


@dataclass
class RFeatureFlags(ROOTSerializable):
    """A class representing the RNTuple Feature Flags.
    RNTuple Feature Flags appear in the Header and Footer Envelopes.
    This class reads the RNTuple Feature Flags from the buffer.
    It also checks if the flags are set for a given feature.
    It aborts reading when an unknown feature is encountered (unknown bit set).
    """

    flags: int
    """The RNTuple Feature Flags (signed 64-bit integer)"""

    @classmethod
    def update_members(cls, members: Members, buffer: ReadBuffer):
        """Reads the RNTuple Feature Flags from the given buffer."""

        # Read the flags from the buffer
        (flags,), buffer = buffer.unpack("<q")  # Signed 64-bit integer

        # There are no feature flags defined for RNTuple yet
        # So abort if any bits are set
        if flags != 0:
            msg = f"Unknown feature flags encountered. int:{flags}; binary:{bin(flags)}"
            raise NotImplementedError(msg)
        members["flags"] = flags
        return members, buffer


@dataclass
class REnvelope(ROOTSerializable):
    """A class representing the RNTuple Envelope.
    An RNTuple Envelope is a data block that contains information about the RNTuple data.
    The following envelope types exist:
    - Header Envelope (0x01): RNTuple schema information (field and column types)
    - Footer Envelope (0x02): Description of clusters
    - Page List Envelope (0x03): Location of data pages
    - Reserved (0x00): Unused and Reserved
    """

    typeID: int
    """The type of the envelope."""
    length: int
    """The length of the envelope (including the envelope header)."""
    checksum: int
    """The checksum of the envelope."""
    _unknown: bytes = field(init=False, repr=False)
    """Unknown bytes at the end of the envelope."""

    @classmethod
    def read(cls, buffer: ReadBuffer) -> tuple[Self, ReadBuffer]:
        """Reads an REnvelope from the given buffer."""
        #### Save initial buffer position (for checking unknown bytes)
        payload_start_pos = buffer.relpos

        #### Get the first 64bit integer (lengthType) which contains the length and type of the envelope
        # lengthType, buffer = buffer.consume(8)
        (lengthType,), buffer = buffer.unpack("<Q")

        # Envelope type, encoded in the 16 least significant bits
        typeID = lengthType & 0xFFFF
        # Check that the typeID matches the class
        if ENVELOPE_TYPE_MAP[typeID] != cls.__name__:
            msg = f"Envelope type {typeID} read does not match passed class {cls.__name__}"
            raise ValueError(msg)

        # Envelope size (uncompressed), encoded in the 48 most significant bits
        length = lengthType >> 16
        # Ensure that the length of the envelope matches the buffer length
        if length - 8 != len(buffer):
            msg = f"Length of envelope ({length} minus 8) of type {typeID} does not match buffer length ({len(buffer)})"
            raise ValueError(msg)

        members = {"typeID": typeID, "length": length}
        #### Get the payload
        members, buffer = cls.update_members(members, buffer)

        #### Consume any unknown trailing information in the envelope
        _unknown, buffer = buffer.consume(
            length - (buffer.relpos - payload_start_pos) - 8
        )
        # Unknown Bytes = Envelope Size - Envelope Bytes Read - Checksum (8 bytes)
        #   Envelope Bytes Read  = buffer.relpos - payload_start_pos

        #### Get the checksum (appended to envelope when writing to disk)
        (checksum,), buffer = buffer.unpack("<Q")  # Last 8 bytes of the envelope
        members["checksum"] = checksum
        envelope = cls(**members)
        envelope._unknown = _unknown
        return envelope, buffer


EnvType = TypeVar("EnvType", bound=REnvelope)


@serializable
class REnvelopeLink(ROOTSerializable):
    """A class representing the RNTuple Envelope Link (somewhat analogous to a TKey).
    An Envelope Link references an Envelope in an RNTuple.
    An Envelope Link is consists of a 64 bit unsigned integer that specifies the
        uncompressed size (i.e. length) of the envelope, followed by a Locator.

    Envelope Links of this form (currently seem to be) only used to locate Page List Envelopes.
    The Header Envelope and Footer Envelope are located using the information in the RNTuple Anchor.
    The Header and Footer Envelope Links are created in the RNTuple Anchor class by casting directly to this class.
    """

    length: Annotated[int, Fmt("<Q")]
    """The uncompressed size of the envelope."""
    locator: RLocator
    """The locator for the envelope."""

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
