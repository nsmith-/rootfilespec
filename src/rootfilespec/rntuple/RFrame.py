import contextlib
import dataclasses
from collections.abc import Iterator
from typing import Any, Generic, TypeVar

from rootfilespec.serializable import (
    ContainerSerDe,
    Members,
    MemberType,
    ReadBuffer,
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


# A frame's members are read from its fSize bytes only, as ROOT reads them
# (RNTupleSerialize.cxx:416-462 at 6.40.04): the spec has readers rely on the
# frame's size (*Frames*, *Notes on Backward and Forward Compatibility*), and
# bytes the members don't use are kept in _unknown.
_LIST_PREAMBLE = 12
"""A list frame's size (8 bytes) and item count (4 bytes)"""


def _split_frame(
    name: str, kind: str, fSize: int, preamble: int, buffer: ReadBuffer
) -> tuple[ReadBuffer, ReadBuffer]:
    """The frame's bytes after its 8-byte size, and the bytes after the frame"""
    if fSize < preamble:
        msg = f"{name}: {kind} frame size {fSize} is smaller than its {preamble}-byte preamble"
        raise ValueError(msg)
    if fSize - 8 > len(buffer):
        msg = f"{name}: {kind} frame size {fSize} runs past the {len(buffer) + 8} bytes left"
        raise ValueError(msg)
    return buffer[: fSize - 8], buffer[fSize - 8 :]


@contextlib.contextmanager
def _bounded(name: str, kind: str, fSize: int) -> Iterator[None]:
    """Report members that need more bytes than their frame holds"""
    try:
        yield
    except IndexError as err:
        msg = f"{name}: the members need more than the {kind} frame's size of {fSize} bytes"
        raise ValueError(msg) from err


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

        #### Read the frame Size and Type
        (fSize,), buffer = buffer.unpack("<q")
        if fSize >= 0:
            msg = f"Expected fSize to be negative, but got {fSize}"
            raise ValueError(msg)
        # abs(fSize) is the uncompressed byte size of frame (including payload)
        fSize = abs(fSize)
        frame_members["fSize"] = fSize
        name = f"{self.cls.__name__} {self.name}" if self.name else self.cls.__name__
        payload, buffer = _split_frame(name, "list", fSize, _LIST_PREAMBLE, buffer)

        with _bounded(name, "list", fSize):
            #### Read the List Frame Items
            (nItems,), payload = payload.unpack("<I")
            items: list[MemberType] = []
            while len(items) < nItems:
                # Read a regular item
                item, payload = self.inner_reader(payload)
                items.append(item)
            frame_members["items"] = items

            # Read the rest of the members
            frame_members, payload = self.cls.update_members(frame_members, payload)

        #### Keep any unknown trailing information in the frame
        _unknown, _ = payload.consume(len(payload))

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
        #### Read the frame Size and Type
        (fSize,), buffer = buffer.unpack("<q")
        if fSize <= 0:
            msg = f"Expected fSize to be positive, but got {fSize}"
            raise ValueError(msg)
        payload, buffer = _split_frame(cls.__name__, "record", fSize, 8, buffer)

        members: Members = {"fSize": fSize}

        #### Read the Record Frame Payload
        with _bounded(cls.__name__, "record", fSize):
            members, payload = cls.update_members(members, payload)

        #### Keep any unknown trailing information in the frame
        _unknown, _ = payload.consume(len(payload))

        frame = cls(**members)
        frame._unknown = _unknown
        return frame, buffer
