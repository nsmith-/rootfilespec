from typing import Annotated

from rootfilespec.bootstrap.streamedobject import StreamedObject
from rootfilespec.buffer import DataFetcher
from rootfilespec.dispatch import DICTIONARY
from rootfilespec.rntuple.envelope import REnvelopeLink
from rootfilespec.rntuple.footer import FooterEnvelope
from rootfilespec.rntuple.header import HeaderEnvelope
from rootfilespec.rntuple.RLocator import LargeLocator
from rootfilespec.serializable import serializable
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

    def get_header(self, fetch_data: DataFetcher) -> HeaderEnvelope:
        """Reads the RNTuple Header Envelope from the given buffer."""
        headerLink = REnvelopeLink(
            self.fLenHeader, LargeLocator(self.fNBytesHeader, self.fSeekHeader)
        )

        return headerLink.read_envelope(fetch_data, HeaderEnvelope)

    def get_footer(self, fetch_data: DataFetcher) -> FooterEnvelope:
        """Reads the RNTuple Footer Envelope from the given buffer."""
        footerLink = REnvelopeLink(
            self.fLenFooter, LargeLocator(self.fNBytesFooter, self.fSeekFooter)
        )

        return footerLink.read_envelope(fetch_data, FooterEnvelope)


DICTIONARY["ROOT3a3aRNTuple"] = ROOT3a3aRNTuple
