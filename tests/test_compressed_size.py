from pathlib import Path

import pytest

from rootfilespec.bootstrap import BOOTSTRAP_CONTEXT
from rootfilespec.bootstrap.compression import RCompressed
from rootfilespec.reader import open_path
from rootfilespec.serializable import BufferContext, ReadBuffer

DATA = Path(__file__).parent.parent / "reference" / "root-io-spec" / "data"
pytestmark = pytest.mark.skipif(
    not DATA.exists(), reason="reference/root-io-spec not checked out"
)


@pytest.mark.parametrize("algorithm", ["zlib", "lzma", "lz4", "zstd"])
def test_compressed_size_is_the_payload_length(algorithm: str):
    """Issue #87: RCompressed.compressed_size() is the length on disk, 9 + the
    compressed size of each block, including LZ4's checksum (root-io-spec
    Compression §4-§5)"""
    path = DATA / "container" / f"compress-{algorithm}.root"
    data = path.read_bytes()
    checked = 0
    with open_path(path) as reader:
        for key in reader.keylist().values():
            header = key.header
            if not header.is_compressed():
                continue
            start = key.fSeekKey + header.fKeylen
            payload = data[start : key.fSeekKey + header.fNbytes]
            buffer = ReadBuffer(
                memoryview(payload), 0, BOOTSTRAP_CONTEXT, BufferContext(abspos=start)
            )
            compressed, rest = RCompressed.read(buffer)
            assert not rest
            assert compressed.compressed_size() == len(payload)
            assert compressed.uncompressed_size() == header.fObjlen
            checked += 1
    assert checked
