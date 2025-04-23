from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Any, Generic, TypeVar

from rootfilespec.bootstrap.envelopebase import (
    REnvelopeLink,
)
from rootfilespec.structutil import (
    Fmt,
    ReadBuffer,
    ROOTSerializable,
    serializable,
)

Item = TypeVar("Item", bound=ROOTSerializable)


@dataclass
class RFrame(ROOTSerializable):
    fSize: int
    _unknown: bytes = field(init=False, repr=False)


@dataclass
class ListFrame(RFrame, Generic[Item]):
    items: list[Item]

    @classmethod
    def read_as(
        cls,
        itemtype: type[Item],
        buffer: ReadBuffer,
    ):
        # Save initial buffer position (for checking unknown bytes)
        start_position = buffer.relpos

        #### Read the frame Size and Type
        (fSize,), buffer = buffer.unpack("<q")
        if fSize >= 0:
            msg = f"Expected fSize to be negative, but got {fSize}"
            raise ValueError(msg)
        # abs(fSize) is the uncompressed byte size of frame (including payload)
        fSize = abs(fSize)

        #### Read the List Frame Items
        (nItems,), buffer = buffer.unpack("<I")
        items: list[Item] = []
        while len(items) < nItems:
            # Read a regular item
            item, buffer = itemtype.read(buffer)
            items.append(item)

        cls_args, buffer = cls.read_members(buffer)

        #### Consume any unknown trailing information in the frame
        _unknown, buffer = buffer.consume(fSize - (buffer.relpos - start_position))
        # Unknown Bytes = Frame Size - Bytes Read
        # Bytes Read = buffer.relpos - start_position

        frame = cls(fSize, items, *cls_args)
        frame._unknown = _unknown
        return frame, buffer

    @classmethod
    def read_members(cls, buffer: ReadBuffer) -> tuple[tuple[Any, ...], ReadBuffer]:
        """Reads extra members from the buffer. This is a placeholder for subclasses to implement."""
        # For now, just return an empty tuple and the buffer unchanged
        return (), buffer

    def __len__(self):
        return len(self.items)

    def __iter__(self):
        return iter(self.items)

    def __getitem__(self, index: int) -> Item:
        return self.items[index]


@dataclass
class RecordFrame(RFrame):
    @classmethod
    def read(cls, buffer: ReadBuffer):
        #### Save initial buffer position (for checking unknown bytes)
        start_position = buffer.relpos

        #### Read the frame Size and Type
        (fSize,), buffer = buffer.unpack("<q")
        if fSize <= 0:
            msg = f"Expected fSize to be positive, but got {fSize}"
            raise ValueError(msg)

        #### Read the Record Frame Payload
        args, buffer = cls.read_members(buffer)

        # abbott: any checks here?

        #### Consume any unknown trailing information in the frame
        _unknown, buffer = buffer.consume(fSize - (buffer.relpos - start_position))
        # Unknown Bytes = Frame Size - Bytes Read
        # Bytes Read = buffer.relpos - start_position

        frame = cls(fSize, *args)
        frame._unknown = _unknown
        return frame, buffer


########################################################################################################################
# Header Envelope Frames

########################################################################################################################
# Footer Envelope Frames


@serializable
class ClusterGroup(RecordFrame):
    """A class representing an RNTuple Cluster Group Record Frame.
    This Record Frame is found in a List Frame in the Footer Envelope of an RNTuple.
    It references the Page List Envelopes for groups of clusters in the RNTuple.

    Attributes:
        fMinEntryNumber (int): The minimum of the first entry number across all of the clusters in the group.
        fEntrySpan (int): The number of entries that are covered by this cluster group.
        fNClusters (int): The number of clusters in the group.
        pagelistLink (REnvelopeLink): Envelope Link to the Page List Envelope for the cluster group.
    """

    fMinEntryNumber: Annotated[int, Fmt("<Q")]
    fEntrySpan: Annotated[int, Fmt("<Q")]
    fNClusters: Annotated[int, Fmt("<I")]
    pagelistLink: REnvelopeLink


@serializable
class SchemaExtension(RecordFrame):
    """A class representing an RNTuple Schema Extension Record Frame.
    This Record Frame is found in the Footer Envelope of an RNTuple.
    It is an extension of the "Schema Description" located in the Header Envelope.
    The schema description is not yet implemented.


    """

    """ The schema extension record frame contains an additional schema description that is incremental with respect to
            the schema contained in the header (see Section Header Envelope). Specifically, it is a record frame with
            the following four fields (identical to the last four fields in Header Envelope):

                List frame: list of field record frames
                List frame: list of column record frames
                List frame: list of alias column record frames
                List frame: list of extra type information

    In general, a schema extension is optional, and thus this record frame might be empty.
        The interpretation of the information contained therein should be identical as if it was found
        directly at the end of the header. This is necessary when fields have been added during writing.

    Note that the field IDs and physical column IDs given by the serialization order should
        continue from the largest IDs found in the header.

    Note that is it possible to extend existing fields by additional column representations.
        This means that columns of the extension header may point to fields of the regular header.
    """


########################################################################################################################
# Page List Envelope Frames


@serializable
class ClusterSummary(RecordFrame):
    """A class representing an RNTuple Cluster Summary Record Frame.
    The Cluster Summary Record Frame is found in the Page List Envelopes of an RNTuple.
    The Cluster Summary Record Frame contains the entry range of a cluster.
    The order of Cluster Summaries defines the cluster IDs, starting from
        the first cluster ID of the cluster group that corresponds to the page list.

    Attributes:
        firstEntryNumber (int): The first entry number in the cluster.
        nEntries (int): The number of entries in the cluster.
        featureFlag (int): The feature flag for the cluster.
    """

    # Notes:
    # Flag 0x01 is reserved for a future specification version that will support sharded clusters.
    # The future use of sharded clusters will break forward compatibility and thus introduce a corresponding feature flag.
    # For now, readers should abort when this flag is set. Other flags should be ignored.

    fFirstEntryNumber: Annotated[int, Fmt("<Q")]
    fNEntriesAndFeatureFlag: Annotated[int, Fmt("<Q")]
    #### Note: nEntries and featureFlag are encoded together in a single 64 bit integer
    # nEntries: int  # The 56 least significant bits of the 64 bit integer
    # featureFlag: int  # The 8 most significant bits of the 64 bit integer

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
