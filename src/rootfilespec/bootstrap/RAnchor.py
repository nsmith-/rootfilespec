from __future__ import annotations

from typing import Annotated

from rootfilespec.bootstrap.envelopebase import (
    REnvelopeLink,
)
from rootfilespec.bootstrap.envelopes import FooterEnvelope, HeaderEnvelope
from rootfilespec.bootstrap.RLocator import LargeLocator
from rootfilespec.bootstrap.TObject import StreamedObject
from rootfilespec.dispatch import DICTIONARY
from rootfilespec.structutil import (
    DataFetcher,
    Fmt,
    serializable,
)


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
        raise NotImplementedError

    def get_footer(self, fetch_data: DataFetcher) -> FooterEnvelope:
        """Reads the RNTuple Footer Envelope from the given buffer."""
        footerLink = REnvelopeLink(
            self.fLenFooter, LargeLocator(self.fNBytesFooter, self.fSeekFooter)
        )

        return footerLink.read_envelope(fetch_data, FooterEnvelope)


DICTIONARY["ROOT3a3aRNTuple"] = ROOT3a3aRNTuple
