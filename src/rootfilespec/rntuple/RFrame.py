import dataclasses
from typing import Any, Generic, TypeVar

from rootfilespec.buffer import ReadBuffer
from rootfilespec.serializable import (
    ContainerSerDe,
    Members,
    MemberType,
    ReadObjMethod,
    ROOTSerializable,
)

Item = TypeVar("Item", bound=ROOTSerializable)


@dataclasses.dataclass
class RFrame(ROOTSerializable):
    """A class representing an RNTuple Frame.
    The ListFrame and RecordFrame classes inherit from this class."""

    fSize: int
    """The size of the frame in bytes. The size is negative for List Frames."""
    _unknown: bytes = dataclasses.field(init=False, repr=False, compare=False)
    """Unknown bytes at the end of the frame."""


@dataclasses.dataclass
class _ListFrameReader:
    cls: type["ListFrame[Any]"]
    name: str
    inner_reader: ReadObjMethod
    """The type of items contained in the List Frame."""

    def __call__(
        self, members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        """Reads a ListFrame from the buffer."""
        frame_members: Members = {}  # Initialize an empty dictionary for frame members

        # Save initial buffer position (for checking unknown bytes)
        start_position = buffer.relpos

        #### Read the frame Size and Type
        (fSize,), buffer = buffer.unpack("<q")
        if fSize >= 0:
            msg = f"Expected fSize to be negative, but got {fSize}"
            raise ValueError(msg)
        # abs(fSize) is the uncompressed byte size of frame (including payload)
        fSize = abs(fSize)
        frame_members["fSize"] = fSize

        #### Read the List Frame Items
        (nItems,), buffer = buffer.unpack("<I")
        items: list[MemberType] = []
        while len(items) < nItems:
            # Read a regular item
            item, buffer = self.inner_reader(buffer)
            items.append(item)
        frame_members["items"] = items

        # members: Members = {"fSize": fSize, "items": items}
        # Read the rest of the members
        frame_members, buffer = self.cls.update_members(frame_members, buffer)

        #### Consume any unknown trailing information in the frame
        _unknown, buffer = buffer.consume(fSize - (buffer.relpos - start_position))
        # Unknown Bytes = Frame Size - Bytes Read
        # Bytes Read = buffer.relpos - start_position

        frame = self.cls(**frame_members)
        frame._unknown = _unknown

        members[self.name] = frame
        return members, buffer


@dataclasses.dataclass
class ListFrame(RFrame, ContainerSerDe, Generic[Item]):
    """A class representing an RNTuple List Frame.
    The List Frame is a container for a list of items of type Item."""

    items: list[Item]
    """The list of items in the List Frame."""

    @classmethod
    def build_reader(cls, fname: str, inner_reader: ReadObjMethod):
        """Build a reader for the ListFrame[Item]."""
        return _ListFrameReader(cls, fname, inner_reader)

    @classmethod
    def update_members(
        cls, members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        """Reads extra members from the buffer. This is a placeholder for subclasses to implement."""
        # For now, just return an empty tuple and the buffer unchanged
        return members, buffer

    def __len__(self):
        return len(self.items)

    def __iter__(self):
        return iter(self.items)

    def __getitem__(self, index: int) -> Item:
        return self.items[index]


@dataclasses.dataclass
class RecordFrame(RFrame):
    """A class representing an RNTuple Record Frame.
    There are many Record Frames, each with a unique format."""

    @classmethod
    def read(cls, buffer: ReadBuffer):
        #### Save initial buffer position (for checking unknown bytes)
        start_position = buffer.relpos

        #### Read the frame Size and Type
        (fSize,), buffer = buffer.unpack("<q")
        if fSize <= 0:
            msg = f"Expected fSize to be positive, but got {fSize}"
            raise ValueError(msg)

        members: Members = {"fSize": fSize}

        #### Read the Record Frame Payload
        members, buffer = cls.update_members(members, buffer)

        #### Consume any unknown trailing information in the frame
        _unknown, buffer = buffer.consume(fSize - (buffer.relpos - start_position))
        # Unknown Bytes = Frame Size - Bytes Read
        # Bytes Read = buffer.relpos - start_position

        frame = cls(**members)
        frame._unknown = _unknown
        return frame, buffer
