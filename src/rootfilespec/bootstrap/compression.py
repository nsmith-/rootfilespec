from enum import IntEnum
from functools import partial
from typing import Annotated, Optional, Protocol

import cramjam  # type: ignore[import-not-found]

from rootfilespec.buffer import ReadBuffer
from rootfilespec.serializable import (
    Members,
    MemberSerDe,
    ROOTSerializable,
    serializable,
)
from rootfilespec.structutil import Fmt


def _read3byte(name: str, members: Members, buffer: ReadBuffer):
    thebytes, buffer = buffer.unpack("3B")
    members[name] = sum(s << (8 * i) for i, s in enumerate(thebytes))
    return members, buffer


class ThreeByteSize(MemberSerDe):
    def build_reader(self, fname: str, ftype: type):
        assert ftype is int
        return partial(_read3byte, fname)


@serializable
class RCompressionHeader(ROOTSerializable):
    """The header of a compressed data payload.

    Attributes:
        fAlgorithm: The compression algorithm used. Possible values are:
            - b"ZL": zlib
            - b"XZ": xz
            - b"L4": lz4
            - b"ZS": zstd
        fVersion: The version of the compression algorithm.
        fCompressedSize: The size of the compressed data, packed into 3 bytes.
            Use compressed_size() to get the actual size.
        fUncompressedSize: The size of the uncompressed data, packed into 3 bytes.
            Use uncompressed_size() to get the actual size.
    """

    fAlgorithm: Annotated[bytes, Fmt("2s")]
    fVersion: Annotated[int, Fmt("B")]
    fCompressedSize: Annotated[int, ThreeByteSize()]
    fUncompressedSize: Annotated[int, ThreeByteSize()]

    def compressed_size(self) -> int:
        if self.fAlgorithm == b"L4":
            # LZ4 has a checksum before the content, so we need to subtract that
            return self.fCompressedSize - 8
        return self.fCompressedSize

    def uncompressed_size(self) -> int:
        return self.fUncompressedSize


class Decompressor(Protocol):
    @staticmethod
    def __call__(input: memoryview, output: memoryview) -> int:
        """Signature of cramjam decompress_into

        Returns the number of bytes written to output.
        May raise cramjam.DecompressionError if decompression fails.
        """
        ...


def _lz4_decompress_into(input: memoryview, output: memoryview) -> int:
    # Workaround for https://github.com/milesgranger/cramjam/issues/216
    tmp = cramjam.lz4.decompress_block(input, output_len=len(output))
    output[: len(tmp)] = tmp
    return len(tmp)


def get_decompressor(algorithm: bytes) -> Decompressor:
    if algorithm == b"ZL":
        return cramjam.zlib.decompress_into  # type: ignore[no-any-return]
    if algorithm == b"XZ":
        return cramjam.xz.decompress_into  # type: ignore[no-any-return]
    if algorithm == b"L4":
        return _lz4_decompress_into
    if algorithm == b"ZS":
        return cramjam.zstd.decompress_into  # type: ignore[no-any-return]
    msg = f"Unknown compression algorithm {algorithm!r}"
    raise NotImplementedError(msg)


@serializable
class RCompressedChunk(ROOTSerializable):
    """A compressed data payload chunk

    Attributes:
        header: The header of the compressed data payload.
        checksum: The checksum of the compressed data payload.
            Only present if the algorithm is L4, as other algorithms have built-in checksums.
        payload: The compressed data payload.
    """

    header: RCompressionHeader
    checksum: Optional[bytes]
    payload: memoryview

    @classmethod
    def update_members(cls, members: Members, buffer: ReadBuffer):
        header, buffer = RCompressionHeader.read(buffer)
        if header.fAlgorithm == b"L4":
            checksum, buffer = buffer.consume(8)
        elif header.fAlgorithm in (b"ZL", b"XZ", b"ZS"):
            checksum = None
        else:
            msg = f"Unknown compression algorithm {header.fAlgorithm!r}"
            raise NotImplementedError(msg)
        # Not using .consume() to avoid copying the payload
        nbytes = header.compressed_size()
        payload, buffer = buffer.consume_view(nbytes)
        members["header"] = header
        members["checksum"] = checksum
        members["payload"] = payload
        return members, buffer

    def decompress_into(self, out: memoryview):
        if self.checksum is not None:
            import xxhash  # type: ignore[import-not-found]

            checksum = xxhash.xxh64(self.payload, seed=0).digest()
            if checksum != self.checksum:
                msg = f"Checksum mismatch: {checksum!r} != {self.checksum!r}"
                raise ValueError(msg)
        decompressor = get_decompressor(self.header.fAlgorithm)
        n = decompressor(self.payload, out)
        if n != self.header.uncompressed_size():
            msg = "Did not read enough"
            raise ValueError(msg)


@serializable
class RCompressed(ROOTSerializable):
    chunks: tuple[RCompressedChunk, ...]

    @classmethod
    def update_members(cls, members: Members, buffer: ReadBuffer):
        chunks: list[RCompressedChunk] = []
        while buffer:
            chunk, buffer = RCompressedChunk.read(buffer)
            chunks.append(chunk)
        members["chunks"] = tuple(chunks)
        return members, buffer

    def compressed_size(self) -> int:
        return sum(chunk.header.compressed_size() for chunk in self.chunks)

    def uncompressed_size(self) -> int:
        return sum(chunk.header.uncompressed_size() for chunk in self.chunks)

    def decompress(self):
        out = memoryview(bytearray(self.uncompressed_size()))
        start = 0
        for chunk in self.chunks:
            length = chunk.header.uncompressed_size()
            chunk.decompress_into(out[start : start + length])
            start += length
        assert start == len(out)
        return out


class CompressionAlgorithm(IntEnum):
    """The compression algorithm used for a column.
    source: https://root.cern/doc/master/Compression_8h_source.html#l00086"""

    # kInherit = -1 # Not used in RNTuple
    # """Inherit the compression algorithm from the parent object."""
    # kUseGlobal = 0 # Not used in RNTuple
    # """Use the global compression algorithm."""
    kUncompressed = 0
    """Is not compressed."""
    kZLIB = 1
    """Use ZLIB compression."""
    kLZMA = 2
    """Use LZMA compression."""
    kOldCompressionAlgo = 3
    """Use Old Jean-loup Gailly's deflation algorithm"""
    kLZ4 = 4
    """Use LZ4 compression."""
    kZSTD = 5
    """Use ZSTD compression."""
    kUndefined = 6
    """Undefined compression algorithm."""

    def __repr__(self) -> str:
        """Get a string representation of this element type."""
        return f"{self.__class__.__name__}.{self.name}"


@serializable
class RCompressionSettings(ROOTSerializable):
    """A class representing the Compression Settings for RNTuple pages/columns.

    If `compressionsettings = 0`, the column is not compressed.

    `compressionsettings = (<compression algorithm> * 100) + <compression level>`
        value: compression algorithm
        - -1: kInherit = -1, // Inherit the compression algorithm from the parent object
        - 0:  kUseGlobal = 0, // Use the global compression algorithm
        - 1:  kZLIB, // Use ZLIB compression
        - 2:  kLZMA, // Use LZMA compression
        - 3:  kOldCompressionAlgo, // Use Old Jean-loup Gailly's deflation algorithm
        - 4:  kLZ4, // Use LZ4 compression
        - 5:  kZSTD, // Use ZSTD compression
        - 6:  kUndefined // Undefined compression algorithm

    e.g. `compressionsettings = 505` means ZSTD compression with level 5.
    source: https://root.cern/doc/master/Compression_8h_source.html#l00086"""

    compressionsettings: Annotated[int, Fmt("<I")]
    """The compression settings for the pages in this column."""

    @property
    def algorithm(self) -> CompressionAlgorithm:
        """Get the compression algorithm used for this column."""
        return CompressionAlgorithm(self.compressionsettings // 100)

    @property
    def level(self) -> int:
        """Get the compression level used for this column."""
        return self.compressionsettings % 100
