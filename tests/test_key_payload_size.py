from pathlib import Path

import pytest

from rootfilespec.reader import open_path
from rootfilespec.serializable import BufferContext, ReadBuffer

DATA = Path(__file__).parent.parent / "reference" / "root-io-spec" / "data"
pytestmark = pytest.mark.skipif(
    not DATA.exists(), reason="reference/root-io-spec not checked out"
)


def _first_key(name: str, compressed: bool):
    path = DATA / "container" / name
    with open_path(path) as reader:
        (key, *_) = [
            key
            for key in reader.keylist().values()
            if key.header.is_compressed() == compressed
        ]
        context = reader.fetch.buffer(key).file_context
    return path.read_bytes(), key, context


def _buffer(data: bytes, context, key, extra: int) -> ReadBuffer:
    """The key's bytes, with ``extra`` bytes more (or, if negative, fewer)"""
    chunk = data[key.offset : key.offset + key.size + extra]
    return ReadBuffer(memoryview(chunk), 0, context, BufferContext(abspos=key.offset))


def test_short_read_raises():
    """Fewer bytes than the key says it stores (a truncated file) are reported
    as such. Before, an uncompressed payload 3 bytes short was taken for a
    compressed one: "Unknown compression algorithm"."""
    data, key, context = _first_key("compress-none-fallback.root", compressed=False)
    stored = key.header.fNbytes - key.header.fKeylen
    with pytest.raises(
        ValueError, match=f"expected {stored} payload bytes, got {stored - 3}"
    ):
        key.read_from(_buffer(data, context, key, -3))


@pytest.mark.parametrize(
    ("name", "compressed"),
    [("compress-none-fallback.root", False), ("compress-zlib.root", True)],
)
def test_longer_fetch_reads_the_same(name: str, compressed: bool):
    """A buffer longer than the key (from a cache, say) reads the same
    object: whether the payload is compressed comes from the key's header,
    not from the buffer's length"""
    data, key, context = _first_key(name, compressed)
    expected = key.read_from(_buffer(data, context, key, 0))
    assert key.read_from(_buffer(data, context, key, 4096)) == expected
