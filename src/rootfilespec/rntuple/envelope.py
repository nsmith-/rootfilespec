from dataclasses import dataclass, field
from typing import Annotated, Generic, TypeVar

import xxhash  # type: ignore[import-not-found]
from typing_extensions import Self

from rootfilespec.bootstrap.compression import decompress
from rootfilespec.bootstrap.RAnchor import ROOT3a3aRNTuple
from rootfilespec.rntuple.RLocator import FileLocator, RLocator, in_file
from rootfilespec.serializable import (
    Members,
    ReadBuffer,
    ROOTSerializable,
    serializable,
)
from rootfilespec.structutil import Fmt

# Map of envelope type to string for printing
ENVELOPE_TYPE_MAP = {0x00: "Reserved"}

FEATURE_FLAGS = {0: "Nested Deferred Columns"}
"""The feature flags the spec defines, by bit (spec v1.0.2.1, *Feature Flags*)"""


@dataclass
class RFeatureFlags(ROOTSerializable):
    """The RNTuple feature flags, in the header and footer envelopes

    Each bit is a forward-incompatible feature that the RNTuple uses, and a
    reader must refuse an RNTuple with a flag it does not know (spec, *Feature
    Flags* and *Notes on Backward and Forward Compatibility*; root-io-spec
    RNTuple NOTES 6). rootfilespec implements none, so reading raises
    ``NotImplementedError`` on any bit set:

    - flag 0, *Nested Deferred Columns*, defined since spec v1.0.2.1. No ROOT
      6.40.04 writer sets it (root-io-spec RNTuple ERRATA 1), and ROOT
      6.40.04's reader refuses it too (``CheckFeatureFlags``,
      ``RNTupleSerialize.cxx:1869-1877``);
    - bits 1 to 62, which the spec does not define;
    - bit 63, which says that flags above 62 are set, in the 64-bit words that
      follow. Those words are not read.
    """

    flags: int
    """The first 64-bit word of the flags (signed), as stored: 0 in every
    RNTuple that reads"""

    @classmethod
    def update_members(cls, members: Members, buffer: ReadBuffer):
        """Reads the RNTuple Feature Flags from the given buffer."""

        # Read the flags from the buffer
        (flags,), buffer = buffer.unpack("<q")  # Signed 64-bit integer

        if flags != 0:
            word = flags & 0xFFFF_FFFF_FFFF_FFFF
            names = [
                f"flag {bit} ({FEATURE_FLAGS[bit]})"
                if bit in FEATURE_FLAGS
                else f"unknown flag {bit}"
                for bit in range(63)
                if word >> bit & 1
            ]
            if word >> 63:
                names.append("flags above 62 (bit 63)")
            msg = f"Unsupported RNTuple feature flags {word:#018x}: {', '.join(names)}"
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


def _uncompressed(
    buffer: ReadBuffer, locator: FileLocator, length: int, what: str
) -> ReadBuffer:
    """The fetched bytes of what a locator and a length link, uncompressed

    ``what`` names the linked block for the errors.
    """
    size = locator.size
    if len(buffer) != size:
        msg = f"{what} at {locator}: expected {size} bytes, got {len(buffer)}"
        raise ValueError(msg)

    # RNTuple decompression tests equality of the stored size (the locator's)
    # and the length: equal means stored raw, smaller compressed, and larger
    # is an error (root-io-spec NOTES 2; RNTupleZip.hxx:106-113)
    if size > length:
        msg = (
            f"{what} at {locator}: stored size {size} is larger than its "
            f"uncompressed length {length}"
        )
        raise ValueError(msg)
    if size < length:
        buffer = decompress(buffer, length)
    return buffer


@dataclass(frozen=True)
class REnvelopeLocator(Generic[EnvType]):
    """A locator for an RNTuple Envelope.

    This follows the locator pattern: it describes where an envelope is located
    and how to deserialize it, but the caller controls when/how to fetch the data.
    """

    length: int
    """The uncompressed length of the envelope."""
    locator: FileLocator
    """The locator for the envelope (offset and size)."""
    envtype: type[EnvType]
    """The envelope type to deserialize."""

    @property
    def offset(self) -> int:
        """The byte offset of the envelope in the file."""
        return self.locator.offset

    @property
    def size(self) -> int:
        """The (compressed) size of the envelope data."""
        return self.locator.size

    def read_from(self, buffer: ReadBuffer) -> EnvType:
        """Read the envelope from the given buffer.

        Envelopes are compressed, so this decompresses and deserializes.
        """
        buffer = _uncompressed(buffer, self.locator, self.length, self.envtype.__name__)

        #### Now read the envelope
        envelope, buffer = self.envtype.read(buffer)

        if buffer:
            msg = "REnvelopeLocator.read_from: buffer not empty after reading envelope."
            raise ValueError(msg)

        return envelope


@dataclass(frozen=True)
class RAnchorLocator:
    """A locator for an RNTuple anchor that a footer links: an attribute set's.

    A linked anchor is found and decompressed like an envelope, from its
    uncompressed length and a locator: ROOT opens it as an ``RNTupleLink``,
    the link envelopes use (``RPageSourceFile::OpenWithDifferentAnchor``,
    ``RPageStorageFile.cxx:369-376`` at 6.40.04). It is read with
    ``ROOT3a3aRNTuple.read``, which verifies its checksum.
    """

    length: int
    """The uncompressed length of the anchor, its checksum included."""
    locator: FileLocator
    """The locator for the anchor object (offset and size)."""

    @property
    def offset(self) -> int:
        """The byte offset of the anchor in the file."""
        return self.locator.offset

    @property
    def size(self) -> int:
        """The (compressed) size of the anchor."""
        return self.locator.size

    def read_from(self, buffer: ReadBuffer) -> ROOT3a3aRNTuple:
        """Read the anchor from the given buffer, decompressing it if needed."""
        buffer = _uncompressed(buffer, self.locator, self.length, "RNTuple anchor")
        anchor, _ = ROOT3a3aRNTuple.read(buffer)
        return anchor


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
        """Get a locator for the envelope.

        Raises NotImplementedError if the envelope's locator is not in the file."""
        return REnvelopeLocator(
            self.length, in_file(self.locator, envtype.__name__), envtype
        )
