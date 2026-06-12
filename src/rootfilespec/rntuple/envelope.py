from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Annotated, Generic, TypeVar, cast

from typing_extensions import Self

from rootfilespec.bootstrap.compression import decompress
from rootfilespec.rntuple.RLocator import RLocator
from rootfilespec.serializable import (
    Locator,
    Members,
    ReadBuffer,
    ROOTSerializable,
    serializable,
)
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

    def __or__(self, other: "RFeatureFlags") -> "RFeatureFlags":
        """Returns a new RFeatureFlags object with the combined flags."""
        return RFeatureFlags(self.flags | other.flags)


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
    _unknown: bytes = field(init=False, repr=False, compare=False)
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


@dataclass(frozen=True)
class REnvelopeLocator(Generic[EnvType]):
    """A locator for an RNTuple Envelope.

    This follows the locator pattern: it describes where an envelope is located
    and how to deserialize it, but the caller controls when/how to fetch the data.
    """

    length: int
    """The uncompressed length of the envelope."""
    locator: RLocator
    """The locator for the envelope (offset and size)."""
    envtype: type[EnvType]
    """The envelope type to deserialize."""

    @property
    def offset(self) -> int:
        """The byte offset of the envelope in the file."""
        # Note: self.locator is always StandardLocator or LargeLocator at runtime,
        # which have offset fields. Cast needed because base RLocator doesn't have offset.
        return cast(Locator[ROOTSerializable], self.locator).offset

    @property
    def size(self) -> int:
        """The (compressed) size of the envelope data."""
        return self.locator.size

    def read_from(self, buffer: ReadBuffer) -> EnvType:
        """Read the envelope from the given buffer.

        Envelopes are compressed, so this decompresses and deserializes.
        """
        #### Decompress the buffer if necessary
        if len(buffer) != self.length:
            buffer = decompress(buffer, self.length)

        #### Now read the envelope
        envelope, buffer = self.envtype.read(buffer)

        if buffer:
            msg = "REnvelopeLocator.read_from: buffer not empty after reading envelope."
            raise ValueError(msg)

        return envelope


@serializable
class REnvelopeLink(ROOTSerializable):
    """A class representing the RNTuple Envelope Link (somewhat analogous to a TKey).

    An Envelope Link references an Envelope in an RNTuple.
    An Envelope Link consists of a 64 bit unsigned integer that specifies the
    uncompressed size (i.e. length) of the envelope, followed by a Locator.

    Envelope Links of this form (currently seem to be) only used to locate Page List Envelopes.
    The Header Envelope and Footer Envelope are located using the information in the RNTuple Anchor.
    """

    length: Annotated[int, Fmt("<Q")]
    """The uncompressed size of the envelope."""
    locator: RLocator
    """The locator for the envelope."""

    def envelope_locator(self, envtype: type[EnvType]) -> REnvelopeLocator[EnvType]:
        """Get a locator for the envelope."""
        return REnvelopeLocator(self.length, self.locator, envtype)

    def read_envelope(
        self,
        fetch_data: Callable[[Locator[ROOTSerializable]], ReadBuffer],
        envtype: type[EnvType],
    ) -> EnvType:
        """Reads the Envelope from the given data source using the locator."""
        loc = self.envelope_locator(envtype)
        buffer = fetch_data(loc)
        return loc.read_from(buffer)
