import dataclasses

from rootfilespec.bootstrap.RAnchor import ROOT3a3aRNTuple
from rootfilespec.buffer import DataFetcher
from rootfilespec.rntuple.envelope import RFeatureFlags
from rootfilespec.rntuple.footer import FooterEnvelope
from rootfilespec.rntuple.header import HeaderEnvelope
from rootfilespec.rntuple.pagelist import PageListEnvelope

@dataclasses.dataclass
class RNTuple():
    """A class representing an RNTuple. 

    # abbott: what do we need this to do?
    - Verify header checksums in footer/pagelist envelopes
    - Extend SchemaDescription from header with footer information
    - Provide method to decompress pages, maybe method for getting specific page
    - Provide method to deserialize pages (or something close)?
    - Should this class take already created envelopes as input, or should it take an anchor and read in the envelopes?

    """
    headerEnvelope: HeaderEnvelope
    footerEnvelope: FooterEnvelope
    pagelistEnvelopes: list[PageListEnvelope]

    @classmethod
    def from_anchor(cls, anchor: ROOT3a3aRNTuple, fetch_data: DataFetcher) -> "RNTuple":
        """Reads the RNTuple from the given anchor."""
        headerEnvelope = anchor.get_header(fetch_data)
        footerEnvelope = anchor.get_footer(fetch_data)

        # Verify header checksum in footer
        if footerEnvelope.headerChecksum != headerEnvelope.checksum:
            raise ValueError(
                f"Header checksum mismatch: {footerEnvelope.headerChecksum} != {headerEnvelope.checksum}"
            )
        pagelistEnvelopes = footerEnvelope.get_pagelists(fetch_data)

        return cls(headerEnvelope, footerEnvelope, pagelistEnvelopes)

    
    @property
    def featureFlags(self) -> RFeatureFlags:
        """Returns the logical or of the feature flags from the header and footer envelopes."""
        return self.headerEnvelope.featureFlags | self.footerEnvelope.featureFlags

    @property
    def _schemaDescription(self):
        """Returns the full schema description, from the header envelope but including footer information."""
        # grab python lists of record frames from header and footer, concatenate them, return tuple
        # leave not implemented for now, as this is not yet used
        msg = "Schema description is not yet implemented."
        raise NotImplementedError(msg)