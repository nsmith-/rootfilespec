from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Annotated

import xxhash  # type: ignore[import-not-found]
from typing_extensions import Self

from rootfilespec.bootstrap.streamedobject import StreamedObject, _StreamConstants
from rootfilespec.serializable import (
    Members,
    ReadBuffer,
    serializable,
)
from rootfilespec.structutil import Fmt

# rootfilespec.rntuple imports rootfilespec.bootstrap (for compression), whose
# __init__ imports this module, so the rntuple names are imported where they are
# used rather than here (#119)
if TYPE_CHECKING:
    from rootfilespec.rntuple.envelope import REnvelopeLocator
    from rootfilespec.rntuple.footer import FooterEnvelope
    from rootfilespec.rntuple.header import HeaderEnvelope

ANCHOR_MIN_SIZE = 78
"""The size of an anchor of class version 2 with its checksum, the smallest
that ROOT reads (``kMinNTupleSize``, ``RMiniFile.cxx:812-818`` at 6.40.04)"""

_CHECKSUM_SIZE = 8
_UNCHECKED_SIZE = 6
"""The byte count and the class version, which the checksum does not cover"""


@dataclass
class _AnchorFrame(StreamedObject):
    """What surrounds the anchor's fields on disk: ROOT::RNTuple's class version
    before them, and the checksum after them, outside the object's byte count
    (root-io-spec RNTuple ERRATA 2 and 3)"""

    fVersionClass: int
    """ROOT::RNTuple's class version, not the format version: 2, or a later one,
    which can only append fields (spec, *Anchor schema*)"""
    checksum: int
    """The anchor's XXH3-64 checksum, as stored (big-endian)"""
    _unknown: bytes = field(init=False, repr=False, compare=False)
    """The fields that a class version after 2 appends, as stored"""


@serializable
class ROOT3a3aRNTuple(_AnchorFrame):
    """The anchor of an RNTuple in a ROOT file: a ``ROOT::RNTuple`` object

    Its members are those of class version 2. Read it with ``read``, from the
    whole uncompressed payload that holds it.
    """

    fVersionEpoch: Annotated[int, Fmt(">H")]
    fVersionMajor: Annotated[int, Fmt(">H")]
    fVersionMinor: Annotated[int, Fmt(">H")]
    fVersionPatch: Annotated[int, Fmt(">H")]
    fSeekHeader: Annotated[int, Fmt(">Q")]
    fNBytesHeader: Annotated[int, Fmt(">Q")]
    fLenHeader: Annotated[int, Fmt(">Q")]
    fSeekFooter: Annotated[int, Fmt(">Q")]
    fNBytesFooter: Annotated[int, Fmt(">Q")]
    fLenFooter: Annotated[int, Fmt(">Q")]
    fMaxKeySize: Annotated[int, Fmt(">Q")]

    @classmethod
    def read(cls, buffer: ReadBuffer) -> tuple[Self, ReadBuffer]:
        """Read the anchor from the whole uncompressed payload that holds it

        The payload is the anchor object, then its checksum: the payload's
        last 8 bytes, whatever its length. A later class version can make it
        longer by appending fields, which are kept as stored
        (``RMiniFileReader::GetNTupleProperAtOffset``, ``RMiniFile.cxx:825-858``
        at 6.40.04). Through a TKey, the payload is the key's fObjLen bytes.
        """
        size = len(buffer)
        if size < ANCHOR_MIN_SIZE:
            msg = (
                f"RNTuple anchor of {size} bytes: class version 2 has "
                f"{ANCHOR_MIN_SIZE} with its checksum, and ROOT reads none shorter"
            )
            raise ValueError(msg)
        anchor, trailer = (
            buffer[: size - _CHECKSUM_SIZE],
            buffer[size - _CHECKSUM_SIZE :],
        )
        (checksum,), rest = trailer.unpack(">Q")

        #### Verify the checksum before trusting the fields, as ROOT does
        # It covers the anchor after its byte count and class version, up to the
        # checksum (root-io-spec RNTuple ERRATA 3; RMiniFile.cxx:580-583, :838-851)
        computed = xxhash.xxh3_64_intdigest(anchor.data[_UNCHECKED_SIZE:])
        if computed != checksum:
            msg = (
                "RNTuple anchor checksum mismatch: "
                f"stored {checksum:#018x}, computed {computed:#018x}"
            )
            raise ValueError(msg)

        #### The object's byte count and class version (ERRATA 2)
        # The checksum lies outside the byte count, so the count spans the rest
        (byteCount, fVersionClass), fields = anchor.unpack(">Ih")
        mask = _StreamConstants.kByteCountMask
        if not byteCount & mask or (byteCount & ~mask) + 4 != len(anchor):
            msg = (
                f"RNTuple anchor byte count {byteCount:#010x} does not span the "
                f"{len(anchor)} bytes before its checksum"
            )
            raise ValueError(msg)
        # ROOT reads class version 2, the first, and later ones, which append
        # fields (RMiniFile.cxx:812-813, :827-829)
        if fVersionClass < 2:
            msg = f"RNTuple anchor of class version {fVersionClass}: the first is 2"
            raise ValueError(msg)

        members: Members = {"fVersionClass": fVersionClass, "checksum": checksum}
        members, tail = cls.update_members(members, fields)

        # Of the format version, only the epoch says whether a reader can read
        # the file (spec, *Versioning Notes*), and ROOT refuses any but 1
        # (RNTupleDescriptorBuilder::SetVersion, RNTupleDescriptor.cxx:1121-1126)
        if members["fVersionEpoch"] != 1:
            version = ".".join(
                str(members[f"fVersion{part}"])
                for part in ("Epoch", "Major", "Minor", "Patch")
            )
            msg = f"RNTuple format version {version}: only epoch 1 is supported"
            raise NotImplementedError(msg)

        #### Keep the fields of a later class version as stored
        _unknown, _ = tail.consume(len(tail))

        out = cls(**members)
        out._unknown = _unknown
        return out, rest

    @property
    def header_locator(self) -> "REnvelopeLocator[HeaderEnvelope]":
        """Get a locator for the RNTuple Header Envelope."""
        from rootfilespec.rntuple.envelope import REnvelopeLocator
        from rootfilespec.rntuple.header import HeaderEnvelope
        from rootfilespec.rntuple.RLocator import LargeLocator

        return REnvelopeLocator(
            self.fLenHeader,
            LargeLocator(self.fNBytesHeader, self.fSeekHeader),
            HeaderEnvelope,
        )

    @property
    def footer_locator(self) -> "REnvelopeLocator[FooterEnvelope]":
        """Get a locator for the RNTuple Footer Envelope."""
        from rootfilespec.rntuple.envelope import REnvelopeLocator
        from rootfilespec.rntuple.footer import FooterEnvelope
        from rootfilespec.rntuple.RLocator import LargeLocator

        return REnvelopeLocator(
            self.fLenFooter,
            LargeLocator(self.fNBytesFooter, self.fSeekFooter),
            FooterEnvelope,
        )
