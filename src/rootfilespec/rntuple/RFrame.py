from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, TypeVar

from rootfilespec.buffer import ReadBuffer
from rootfilespec.serializable import Members, ROOTSerializable

Item = TypeVar("Item", bound=ROOTSerializable)


@dataclass
class RFrame(ROOTSerializable):
    """A class representing an RNTuple Frame.
    The ListFrame and RecordFrame classes inherit from this class."""

    fSize: int
    """The size of the frame in bytes. The size is negative for List Frames."""
    _unknown: bytes = field(init=False, repr=False)
    """Unknown bytes at the end of the frame."""


@dataclass
class ListFrame(RFrame, Generic[Item]):
    """A class representing an RNTuple List Frame.
    The List Frame is a container for a list of items of type Item."""

    items: list[Item]
    """The list of items in the List Frame."""

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

        members: Members = {"fSize": fSize, "items": items}
        # Read the rest of the members
        members, buffer = cls.update_members(members, buffer)

        #### Consume any unknown trailing information in the frame
        _unknown, buffer = buffer.consume(fSize - (buffer.relpos - start_position))
        # Unknown Bytes = Frame Size - Bytes Read
        # Bytes Read = buffer.relpos - start_position

        frame = cls(**members)
        frame._unknown = _unknown
        return frame, buffer

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


@dataclass
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

        # abbott: any checks here?

        #### Consume any unknown trailing information in the frame
        _unknown, buffer = buffer.consume(fSize - (buffer.relpos - start_position))
        # Unknown Bytes = Frame Size - Bytes Read
        # Bytes Read = buffer.relpos - start_position

        frame = cls(**members)
        frame._unknown = _unknown
        return frame, buffer
