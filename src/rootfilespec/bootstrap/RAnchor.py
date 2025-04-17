from __future__ import annotations

from dataclasses import dataclass

from ..structutil import (
    ReadBuffer,
    ROOTSerializable,
    StructClass,
    sfield,
    structify,
)
from .RNTupleEnvelopeLink import (
    RNTupleEnvelopeLink,
    RNTupleLocator,
    RNTupleStandardLocator,
)
from .streamedobject import StreamHeader
from .TKey import DICTIONARY


@structify(big_endian=True)
@dataclass(order=True)
class RNTupleVersion(StructClass):
    """A class representing the RNTuple Version

    Attributes:
        fVersionEpoch (int): Version Epoch
        fVersionMajor (int): Version Major
        fVersionMinor (int): Version Minor
        fVersionPatch (int): Version Patch
    """

    # fVersionEpoch: int = sfield("H")
    # fVersionMajor: int = sfield("H")
    # fVersionMinor: int = sfield("H")
    # fVersionPatch: int = sfield("H")

    epoch: int = sfield("H")
    major: int = sfield("H")
    minor: int = sfield("H")
    patch: int = sfield("H")


@dataclass
class RNTupleAnchor(ROOTSerializable):
    """RNTuple Anchor object

    Attributes:
        sheader (StreamHeader): Stream Header information
        fVersion (RNTupleVersion): RNTuple Version
        headerLink (RNTupleEnvelopeLink): Envelope Link to the RNTuple Header Envelope
        footerLink (RNTupleEnvelopeLink): Envelope Link to the RNTuple Footer Envelope
        fMaxKeySize (int): Maximum size of an RBlob
        unknown (bytes): Unknown bytes after the Anchor, before the checksum
        checksum (int): Checksum of the Anchor
    """

    sheader: StreamHeader
    fVersion: RNTupleVersion
    headerLink: RNTupleEnvelopeLink
    # fSeekHeader (int): Offset to the header envelope
    # fNBytesHeader (int): Compressed size of the header envelope
    # fLenHeader (int): Uncompressed size of the header envelope
    footerLink: RNTupleEnvelopeLink
    # fSeekFooter (int): Offset to the footer envelope
    # fNBytesFooter (int): Compressed size of the footer envelope
    # fLenFooter (int): Uncompressed size of the footer envelope
    fMaxKeySize: int
    unknown: bytes | None
    checksum: int

    @classmethod
    def read(cls, buffer: ReadBuffer):
        #### Read the StreamHeader (every named class has a StreamHeader)
        sheader, buffer = StreamHeader.read(buffer)

        #### Read the RNTuple Version
        fVersion, buffer = RNTupleVersion.read(buffer)

        #### Read the RNTuple Header Envelope Link
        (fSeekHeader, fNBytesHeader, fLenHeader), buffer = buffer.unpack(">QQQ")

        headerLink = RNTupleEnvelopeLink(
            fLenHeader,
            RNTupleLocator(True, RNTupleStandardLocator(fNBytesHeader, fSeekHeader)),
        )

        # Verify the header link (In case RNTupleEnvelopeLink class gets updated with more complicated formats)
        if fLenHeader != headerLink.length:
            msg = f"RNTupleAnchor.read: {fLenHeader=} != {headerLink.length=}"
            raise ValueError(msg)
        if fNBytesHeader != headerLink.locator.locatorSubclass.size:
            msg = f"RNTupleAnchor.read: {fNBytesHeader=} != {headerLink.locator.locatorSubclass.size=}"
            raise ValueError(msg)
        if fSeekHeader != headerLink.locator.locatorSubclass.offset:
            msg = f"RNTupleAnchor.read: {fSeekHeader=} != {headerLink.locator.locatorSubclass.offset=}"
            raise ValueError(msg)

        #### Read the RNTuple Footer Envelope Link
        (fSeekFooter, fNBytesFooter, fLenFooter), buffer = buffer.unpack(">QQQ")

        footerLink = RNTupleEnvelopeLink(
            fLenFooter,
            RNTupleLocator(True, RNTupleStandardLocator(fNBytesFooter, fSeekFooter)),
        )

        # Verify the footer link (In case RNTupleEnvelopeLink class gets updated with more complicated formats)
        if fLenFooter != footerLink.length:
            msg = f"RNTupleAnchor.read: {fLenFooter=} != {footerLink.length=}"
            raise ValueError(msg)
        if fNBytesFooter != footerLink.locator.locatorSubclass.size:
            msg = f"RNTupleAnchor.read: {fNBytesFooter=} != {footerLink.locator.locatorSubclass.size=}"
            raise ValueError(msg)
        if fSeekFooter != footerLink.locator.locatorSubclass.offset:
            msg = f"RNTupleAnchor.read: {fSeekFooter=} != {footerLink.locator.locatorSubclass.offset=}"
            raise ValueError(msg)

        #### Read the Maximum Key Size
        (fMaxKeySize,), buffer = buffer.unpack(">Q")

        """ TODO: Use fMaxKeySize to check if the RBlobs are too big
                    Max Key Size represents the maximum size of an RBlob (associated to one TFile key).
                    Payloads bigger than that size will be written as multiple RBlobs/TKeys, and the
                    offsets of all but the first RBlob will be written at the end of the first one.
                    This allows bypassing the inherent TKey size limit of 1 GiB.
        """

        #### Consume any unknown trailing information in the anchor
        unknown, buffer = buffer.consume(buffer.__len__() - 8)
        # The buffer contains exactly and only the anchor bytes (including the checksum)

        #### Get the checksum (appended to anchor when writing to disk)
        (checksum,), buffer = buffer.unpack(">Q")  # Last 8 bytes of the anchor

        return cls(
            sheader, fVersion, headerLink, footerLink, fMaxKeySize, unknown, checksum
        ), buffer


DICTIONARY[b"ROOT::RNTuple"] = RNTupleAnchor
