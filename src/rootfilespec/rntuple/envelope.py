from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Annotated, Generic, TypeVar, cast

import xxhash  # type: ignore[import-not-found]
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
        #### Get the first 64bit integer (lengthType) which contains the length and type of the envelope
        (lengthType,), _ = buffer.unpack("<Q")

        # Envelope type, encoded in the 16 least significant bits
        typeID = lengthType & 0xFFFF
        # Check that the typeID matches the class
        if ENVELOPE_TYPE_MAP[typeID] != cls.__name__:
            msg = f"Envelope type {typeID} read does not match passed class {cls.__name__}"
            raise ValueError(msg)

        # Envelope size (uncompressed), encoded in the 48 most significant bits.
        # It includes the preamble and the checksum, so it is at least 16 bytes
        length = lengthType >> 16
        if length < 16:
            msg = f"Length of envelope ({length}) of type {typeID} is shorter than 16 bytes"
            raise ValueError(msg)
        if length != len(buffer):
            msg = f"Length of envelope ({length}) of type {typeID} does not match buffer length ({len(buffer)})"
            raise ValueError(msg)

        #### Split the envelope once: the bytes the checksum covers, then the checksum
        # The checksum covers [0, length - 8): the preamble, the payload and any
        # unknown trailing bytes (root-io-spec ERRATA 5; "Checksum verification
        # ... must include both known and unknown contents")
        covered, trailer = buffer[: length - 8], buffer[length - 8 :]
        (checksum,), rest = trailer.unpack("<Q")

        #### Verify it before trusting the payload, as ROOT does
        # (RNTupleSerialize.cxx:909-939)
        computed = xxhash.xxh3_64_intdigest(covered.data)
        if computed != checksum:
            msg = (
                f"{cls.__name__} checksum mismatch: "
                f"stored {checksum:#018x}, computed {computed:#018x}"
            )
            raise ValueError(msg)

        #### Get the payload, after the 8-byte preamble
        _, payload = covered.consume(8)
        members = {"typeID": typeID, "length": length, "checksum": checksum}
        members, payload = cls.update_members(members, payload)

        #### Keep any unknown trailing information in the envelope
        _unknown, _ = payload.consume(len(payload))

        envelope = cls(**members)
        envelope._unknown = _unknown
        return envelope, rest


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
        if len(buffer) != self.size:
            msg = (
                f"{self.envtype.__name__} at {self.locator}: expected {self.size} "
                f"bytes, got {len(buffer)}"
            )
            raise ValueError(msg)

        #### Decompress the buffer if necessary
        # RNTuple decompression tests equality of the stored size (the locator's)
        # and the length: equal means stored raw, smaller compressed, and larger
        # is an error (root-io-spec NOTES 2; RNTupleZip.hxx:106-113)
        if self.size > self.length:
            msg = (
                f"{self.envtype.__name__} at {self.locator}: stored size "
                f"{self.size} is larger than its uncompressed length {self.length}"
            )
            raise ValueError(msg)
        if self.size < self.length:
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
