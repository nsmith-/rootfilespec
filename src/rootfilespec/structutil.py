from __future__ import annotations

import dataclasses
import struct
from typing import Any, Callable, TypeVar, get_args, get_origin, get_type_hints


@dataclasses.dataclass
class ReadBuffer:
    """A ReadBuffer is a memoryview that keeps track of the absolute and relative
    positions of the data it contains.

    Attributes:
        data (memoryview): The data contained in the buffer.
        abspos (int | None): The absolute position of the buffer in the file.
            If the buffer was created from a compressed buffer, this will be None.
        relpos (int): The relative position of the buffer from the start of the TKey.
        local_refs (dict[int, bytes], optional): A dictionary of local references that may
            be found in the buffer (for use reading StreamHeader data)
    """

    data: memoryview
    abspos: int | None
    relpos: int
    local_refs: dict[int, bytes] = dataclasses.field(default_factory=dict)

    def __getitem__(self, key: slice) -> ReadBuffer:
        """Get a slice of the buffer."""
        if key.start > len(self.data):
            msg = f"Cannot get slice {key} from buffer of length {len(self.data)}"
            raise IndexError(msg)
        return ReadBuffer(
            self.data[key],
            self.abspos + key.start if self.abspos is not None else None,
            self.relpos + key.start,
            self.local_refs,
        )

    def __len__(self) -> int:
        """Get the length of the buffer."""
        return len(self.data)

    def __repr__(self) -> str:
        """Get a string representation of the buffer."""
        return (
            f"ReadBuffer size {len(self.data)} at abspos={self.abspos}, relpos={self.relpos}"
            "\n  local_refs: "
            + "".join(f"\n    {k}: {v!r}" for k, v in self.local_refs.items())
            + "\n  data[:0x100]: "
            + "".join(
                f"\n    0x{i:03x} | "
                + self.data[i : i + 16].hex(sep=" ")
                + " | "
                + "".join(
                    chr(c) if 32 <= c < 127 else "." for c in self.data[i : i + 16]
                )
                for i in range(0, 256, 16)
            )
        )

    def unpack(self, fmt: str | struct.Struct) -> tuple[tuple[Any, ...], ReadBuffer]:
        """Unpack the buffer according to the given format."""
        if isinstance(fmt, struct.Struct):
            return fmt.unpack(self.data[: fmt.size]), self[fmt.size :]
        size = struct.calcsize(fmt)
        return struct.unpack(fmt, self.data[:size]), self[size:]

    def consume(self, size: int) -> tuple[bytes, ReadBuffer]:
        """Consume the given number of bytes from the buffer."""
        return bytes(self.data[:size]), self[size:]


DataFetcher = Callable[[int, int], ReadBuffer]

T = TypeVar("T", bound="ROOTSerializable")


class ROOTSerializable:
    @classmethod
    def read(cls: type[T], buffer: ReadBuffer) -> tuple[T, ReadBuffer]:
        args = []
        namespace = get_type_hints(cls)
        for field in dataclasses.fields(cls):  # type: ignore[arg-type]
            ftype = namespace[field.name]
            if isinstance(ftype, type) and issubclass(ftype, ROOTSerializable):
                arg, buffer = ftype.read(buffer)
            elif get_origin(ftype) is ListFrame:
                itemtype = get_args(ftype)[0]
                arg, buffer = ListFrame.read(buffer, itemtype)
            else:
                msg = f"Cannot read field {field.name} of type {ftype} (type: {type(ftype)})"
                raise NotImplementedError(msg)
            args.append(arg)
        return cls(*args), buffer


S = TypeVar("S", bound="StructClass")


class StructClass(ROOTSerializable):
    _struct: struct.Struct

    @classmethod
    def size(cls) -> int:
        return cls._struct.size

    @classmethod
    def read(cls: type[S], buffer: ReadBuffer) -> tuple[S, ReadBuffer]:
        args, buffer = buffer.unpack(cls._struct)
        return cls(*args), buffer


def structify(big_endian: bool):
    """A decorator to add a precompiled struct.Struct object to a StructClass."""

    endianness = ">" if big_endian else "<"

    def decorator(cls):
        fmt = "".join(f.metadata["format"] for f in dataclasses.fields(cls))
        cls._struct = struct.Struct(endianness + fmt)
        return cls

    return decorator


def sfield(fmt: str):
    """A dataclass field that has a struct format."""
    return dataclasses.field(metadata={"format": fmt})


@dataclasses.dataclass
class ListFrame[Item: ROOTSerializable]:
    size: int
    nitems: int
    items: list[Item]

    @classmethod
    def read(cls, buffer: ReadBuffer, itemtype: type[Item]) -> tuple[ListFrame[Item], ReadBuffer]:
        (size,), buffer = buffer.unpack(">i")
        (nitems,), buffer = buffer.unpack(">i")
        items: list[Item] = []
        for _ in range(nitems):
            item, buffer = itemtype.read(buffer)
            items.append(item)
        return cls(size, nitems, items), buffer


@structify(big_endian=True)
@dataclasses.dataclass
class PageLink(StructClass):
    offset: int = sfield("i")
    nbytes: int = sfield("i")


@dataclasses.dataclass
class Cluster(ROOTSerializable):
    pages: ListFrame[PageLink]


if __name__ == "__main__":
    expected = Cluster(ListFrame(16, 2, [PageLink(32, 32), PageLink(64, 32)]))

    data = (
        b"\x00\x00\x00\x10"  # size
        b"\x00\x00\x00\x02"  # nitems
        b"\x00\x00\x00\x20"  # item1 offset
        b"\x00\x00\x00\x20"  # item1 nbytes
        b"\x00\x00\x00\x40"  # item2 offset
        b"\x00\x00\x00\x20"  # item2 nbytes
    )

    cluster, remaining = Cluster.read(ReadBuffer(memoryview(data), 0, 0))
    assert remaining.data == b""
    assert cluster == expected
    print(cluster)
