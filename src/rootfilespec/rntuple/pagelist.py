from collections.abc import Callable
from typing import Annotated

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
from rootfilespec.serializable import (
    Locator,
    Members,
    ReadBuffer,
    ROOTSerializable,
    serializable,
)
from rootfilespec.structutil import Fmt


@serializable
class ClusterSummary(RecordFrame):
    """A class representing an RNTuple Cluster Summary Record Frame.
    The Cluster Summary Record Frame is found in the Page List Envelopes of an RNTuple.
    The Cluster Summary Record Frame contains the entry range of a cluster.
    The order of Cluster Summaries defines the cluster IDs, starting from
        the first cluster ID of the cluster group that corresponds to the page list.
    """

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

    @classmethod
    def update_members(cls, members: Members, buffer: ReadBuffer):
        (fFirstEntryNumber, fNEntriesAndFeatureFlag), buffer = buffer.unpack("<QQ")
        # Flag 0x01 is reserved for sharded clusters, a future and incompatible
        # format: readers abort when it is set, and ignore other flags (spec,
        # *Cluster Summary Record Frame*; RNTupleSerialize.cxx:1239-1240)
        if (fNEntriesAndFeatureFlag >> 56) & 0x01:
            msg = (
                f"Cluster summary of entries from {fFirstEntryNumber} has the "
                "sharded-cluster flag (0x01) set; sharded clusters are not supported"
            )
            raise NotImplementedError(msg)
        members["fFirstEntryNumber"] = fFirstEntryNumber
        members["fNEntriesAndFeatureFlag"] = fNEntriesAndFeatureFlag
        return members, buffer


@serializable
class PageListEnvelope(REnvelope):
    """A class representing the RNTuple Page List Envelope payload structure."""

    headerChecksum: Annotated[int, Fmt("<Q")]
    """Checksum of the Header Envelope"""
    clusterSummaries: ListFrame[ClusterSummary]
    """The List Frame of Cluster Summary Record Frames"""
    pageLocations: ListFrame[ListFrame[PageLocations[RPageDescription]]]
    """The Page Locations Triple Nested List Frame"""

    @property
    def page_locators(self) -> list[list[list[RPageDescription]]]:
        """Get locators for all pages in this page list.

        Returns a triple-nested list structure:
        - Top level: clusters
        - Middle level: columns
        - Inner level: pages
        """
        return [
            [list(pagelist) for pagelist in columnlist]
            for columnlist in self.pageLocations
        ]

    def get_pages(self, fetch_data: Callable[[Locator[ROOTSerializable]], ReadBuffer]):
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
