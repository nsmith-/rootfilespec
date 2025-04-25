from typing import Annotated

from rootfilespec.buffer import DataFetcher, ReadBuffer
from rootfilespec.rntuple.envelope import (
    ENVELOPE_TYPE_MAP,
    REnvelope,
)
from rootfilespec.rntuple.pagelocations import ClusterLocations
from rootfilespec.rntuple.RFrame import ListFrame, RecordFrame
from rootfilespec.rntuple.RPage import RPage
from rootfilespec.serializable import Members, serializable
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
    pageLocations: ClusterLocations
    """The Page Locations Triple Nested List Frame"""

    @classmethod
    def update_members(cls, members: Members, buffer: ReadBuffer):
        """Reads the RNTuple Page List Envelope payload from the given buffer."""
        # Read the header checksum
        (headerChecksum,), buffer = buffer.unpack("<Q")

        # Read the cluster summary list frame
        clusterSummaries, buffer = ListFrame.read_as(ClusterSummary, buffer)

        # Read the page locations
        pageLocations, buffer = ClusterLocations.read(buffer)

        members["headerChecksum"] = headerChecksum
        members["clusterSummaries"] = clusterSummaries
        members["pageLocations"] = pageLocations
        return members, buffer

    def get_pages(self, fetch_data: DataFetcher):
        """Get the RNTuple Pages from the Page Locations Nested List Frame."""
        #### Get the Page Locations
        page_locations: list[list[list[RPage]]] = []

        for i_column, columnlist in enumerate(self.pageLocations):
            page_locations.append([])
            for i_page, pagelist in enumerate(columnlist):
                page_locations[i_column].append([])
                for page_description in pagelist:
                    # Read the page from the buffer
                    page = page_description.get_page(fetch_data)
                    # Append the page to the list
                    page_locations[i_column][i_page].append(page)

        return page_locations


ENVELOPE_TYPE_MAP[0x03] = "PageListEnvelope"
