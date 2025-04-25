from typing import Annotated, Optional, Protocol

import cramjam  # type: ignore[import-not-found]

from rootfilespec.buffer import ReadBuffer
from rootfilespec.serializable import Members, ROOTSerializable, serializable
from rootfilespec.structutil import Fmt


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
    # TODO: Make this a 3-byte integer using a custom MemberSerDe
    fCompressedSize: Annotated[bytes, Fmt("3s")]
    fUncompressedSize: Annotated[bytes, Fmt("3s")]

    def compressed_size(self) -> int:
        out = sum(s << (8 * i) for i, s in enumerate(self.fCompressedSize))
        if self.fAlgorithm == b"L4":
            #  LZ4 doesn't account for the checksum, so we need to subtract that
            out -= 8
        return out

    def uncompressed_size(self) -> int:
        return sum(s << (8 * i) for i, s in enumerate(self.fUncompressedSize))


class Decompressor(Protocol):
    def __call__(self, data: memoryview, output_len: Optional[int] = None) -> bytes: ...


def get_decompressor(algorithm: bytes) -> Decompressor:
    if algorithm == b"ZL":
        return cramjam.zlib.decompress  # type: ignore[no-any-return]
    if algorithm == b"XZ":
        return cramjam.xz.decompress  # type: ignore[no-any-return]
    if algorithm == b"L4":
        return cramjam.lz4.decompress_block  # type: ignore[no-any-return]
    if algorithm == b"ZS":
        return cramjam.zstd.decompress  # type: ignore[no-any-return]
    msg = f"Unknown compression algorithm {algorithm!r}"
    raise NotImplementedError(msg)


@serializable
class RCompressed(ROOTSerializable):
    """A compressed data payload.

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
        else:
            checksum = None
        # Not using .consume() to avoid copying the payload
        nbytes = header.compressed_size()
        payload, buffer = buffer.consume_view(nbytes)
        members["header"] = header
        members["checksum"] = checksum
        members["payload"] = payload
        return members, buffer

    def decompress(self) -> memoryview:
        if self.checksum is not None:
            import xxhash  # type: ignore[import-not-found]

            checksum = xxhash.xxh64(self.payload, seed=0).digest()
            if checksum != self.checksum:
                msg = f"Checksum mismatch: {checksum!r} != {self.checksum!r}"
                raise ValueError(msg)
        decompressor = get_decompressor(self.header.fAlgorithm)
        out = decompressor(self.payload, output_len=self.header.uncompressed_size())
        return memoryview(out)
