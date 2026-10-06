from dataclasses import dataclass
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

_CHECKSUM_SIZE = 8
_UNCHECKED_SIZE = 6
"""The byte count and the class version, which the checksum does not cover"""


@dataclass
class _AnchorFrame(StreamedObject):
    """What follows the anchor's fields on disk: its checksum, outside the
    object's byte count (root-io-spec RNTuple ERRATA 3)"""

    checksum: int
    """The anchor's XXH3-64 checksum, as stored (big-endian)"""


@serializable
class ROOT3a3aRNTuple(_AnchorFrame):
    """The anchor of an RNTuple in a ROOT file: a ``ROOT::RNTuple`` object

    Read it with ``read``, from the whole uncompressed payload that holds it.
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

        The payload is the anchor object, then its checksum in the payload's
        last 8 bytes (``RMiniFileReader::GetNTupleProperAtOffset``,
        ``RMiniFile.cxx:825-858`` at 6.40.04). Through a TKey, the payload is
        the key's fObjLen bytes.
        """
        size = len(buffer)
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
        (byteCount, _), fields = anchor.unpack(">Ih")
        mask = _StreamConstants.kByteCountMask
        if not byteCount & mask or (byteCount & ~mask) + 4 != len(anchor):
            msg = (
                f"RNTuple anchor byte count {byteCount:#010x} does not span the "
                f"{len(anchor)} bytes before its checksum"
            )
            raise ValueError(msg)

        members: Members = {"checksum": checksum}
        members, tail = cls.update_members(members, fields)
        if tail:
            msg = f"RNTuple anchor: {len(tail)} bytes after its fields"
            raise ValueError(msg)

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

        return cls(**members), rest

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
