import struct
from typing import Any, Callable, Optional


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
    abspos: Optional[int]
    relpos: int
    local_refs: dict[int, bytes]

    def __init__(
        self,
        data: memoryview,
        abspos: Optional[int],
        relpos: int,
        local_refs: Optional[dict[int, bytes]] = None,
    ):
        self.data = data
        self.abspos = abspos
        self.relpos = relpos
        self.local_refs = {} if local_refs is None else local_refs

    def __getitem__(self, key: slice):
        """Get a slice of the buffer."""
        start: int = key.start or 0
        if start > len(self.data):
            msg = f"Cannot get slice {key} from buffer of length {len(self.data)}"
            raise IndexError(msg)
        return ReadBuffer(
            self.data[key],
            self.abspos + start if self.abspos is not None else None,
            self.relpos + start,
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
                for i in range(0, min(256, len(self)), 16)
            )
        )

    def __bool__(self) -> bool:
        return bool(self.data)

    def unpack(self, fmt: str) -> tuple[tuple[Any, ...], "ReadBuffer"]:
        """Unpack the buffer according to the given format."""
        size = struct.calcsize(fmt)
        out = struct.unpack(fmt, self.data[:size])
        return out, self[size:]

    def consume(self, size: int) -> tuple[bytes, "ReadBuffer"]:
        """Consume the given number of bytes from the buffer.

        Returns a copy of the data and the remaining buffer.
        """
        if size < 0:
            msg = (
                f"Cannot consume a negative number of bytes: {size=}, {self.__len__()=}"
            )
            raise ValueError(msg)
        out = self.data[:size].tobytes()
        return out, self[size:]

    def consume_view(self, size: int) -> tuple[memoryview, "ReadBuffer"]:
        """Consume the given number of bytes and return a view (not a copy).

        Use consume() to get a copy.
        """
        return self.data[:size], self[size:]


DataFetcher = Callable[[int, int], ReadBuffer]
