from typing import Annotated

from rootfilespec.buffer import DataFetcher
from rootfilespec.rntuple.envelope import (
    ENVELOPE_TYPE_MAP,
    REnvelope,
)
from rootfilespec.rntuple.pagelocations import (
    PageLocations,
    RPageDescription,
)
from rootfilespec.rntuple.RFrame import ListFrame, RecordFrame
from rootfilespec.rntuple.RPage import RPage
from rootfilespec.serializable import serializable
from rootfilespec.structutil import Fmt


@serializable
class ClusterSummary(RecordFrame):
    """A class representing an RNTuple Cluster Summary Record Frame.
    The Cluster Summary Record Frame is found in the Page List Envelopes of an RNTuple.
    The Cluster Summary Record Frame contains the entry range of a cluster.
    The order of Cluster Summaries defines the cluster IDs, starting from
        the first cluster ID of the cluster group that corresponds to the page list.
    """

    # Notes:
    # Flag 0x01 is reserved for a future specification version that will support sharded clusters.
    # The future use of sharded clusters will break forward compatibility and thus introduce a corresponding feature flag.
    # For now, readers should abort when this flag is set. Other flags should be ignored.

    fFirstEntryNumber: Annotated[int, Fmt("<Q")]
    """The first entry number in the cluster."""
    fNEntriesAndFeatureFlag: Annotated[int, Fmt("<Q")]
    """The number of entries in the cluster and the feature flag for the cluster, encoded together in a single 64 bit integer.
    The 56 least significant bits of the 64 bit integer are the number of entries in the cluster.
    The 8 most significant bits of the 64 bit integer are the feature flag for the cluster."""

    @property
    def fNEntries(self) -> int:
        """The number of entries in the cluster."""
        # The 56 least significant bits of the 64 bit integer
        return self.fNEntriesAndFeatureFlag & 0x00FFFFFFFFFFFFFF

    @property
    def fFeatureFlag(self) -> int:
        """The feature flag for the cluster."""
        # The 8 most significant bits of the 64 bit integer
        return (self.fNEntriesAndFeatureFlag >> 56) & 0xFF


@serializable
class PageListEnvelope(REnvelope):
    """A class representing the RNTuple Page List Envelope payload structure."""

    headerChecksum: Annotated[int, Fmt("<Q")]
    """Checksum of the Header Envelope"""
    clusterSummaries: ListFrame[ClusterSummary]
    """The List Frame of Cluster Summary Record Frames"""
    pageLocations: ListFrame[ListFrame[PageLocations[RPageDescription]]]
    """The Page Locations Triple Nested List Frame"""

    def get_pages(self, fetch_data: DataFetcher):
        """Get the RNTuple Pages from the Page Locations Nested List Frame.
        Does not decompress the pages."""
        #### Get the Page Locations
        pages: list[list[list[RPage]]] = [
            [
                [page_description.get_page(fetch_data) for page_description in pagelist]
                for pagelist in columnlist
            ]
            for columnlist in self.pageLocations
        ]

        return pages


ENVELOPE_TYPE_MAP[0x03] = "PageListEnvelope"
