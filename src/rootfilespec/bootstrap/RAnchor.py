from collections.abc import Callable
from typing import Annotated

from rootfilespec.bootstrap.streamedobject import StreamedObject
from rootfilespec.rntuple.envelope import REnvelopeLocator
from rootfilespec.rntuple.footer import FooterEnvelope
from rootfilespec.rntuple.header import HeaderEnvelope
from rootfilespec.rntuple.RLocator import LargeLocator
from rootfilespec.serializable import (
    Locator,
    ReadBuffer,
    ROOTSerializable,
    serializable,
)
from rootfilespec.structutil import Fmt


@serializable
class ROOT3a3aRNTuple(StreamedObject):
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

    @property
    def header_locator(self) -> REnvelopeLocator[HeaderEnvelope]:
        """Get a locator for the RNTuple Header Envelope."""
        return REnvelopeLocator(
            self.fLenHeader,
            LargeLocator(self.fNBytesHeader, self.fSeekHeader),
            HeaderEnvelope,
        )

    @property
    def footer_locator(self) -> REnvelopeLocator[FooterEnvelope]:
        """Get a locator for the RNTuple Footer Envelope."""
        return REnvelopeLocator(
            self.fLenFooter,
            LargeLocator(self.fNBytesFooter, self.fSeekFooter),
            FooterEnvelope,
        )

    def get_header(
        self, fetch_data: Callable[[Locator[ROOTSerializable]], ReadBuffer]
    ) -> HeaderEnvelope:
        """Reads the RNTuple Header Envelope from the given buffer."""
        loc = self.header_locator
        buffer = fetch_data(loc)
        return loc.read_from(buffer)

    def get_footer(
        self, fetch_data: Callable[[Locator[ROOTSerializable]], ReadBuffer]
    ) -> FooterEnvelope:
        """Reads the RNTuple Footer Envelope from the given buffer."""
        loc = self.footer_locator
        buffer = fetch_data(loc)
        return loc.read_from(buffer)
