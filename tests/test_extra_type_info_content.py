from pathlib import Path

import pytest

from rootfilespec.reader import open_path
from rootfilespec.rntuple.RNTuple import RNTuple

DATA = Path(__file__).parent.parent / "reference" / "root-io-spec" / "data" / "rntuple"


@pytest.mark.skipif(not DATA.exists(), reason="reference/root-io-spec not checked out")
def test_streamer_info_content():
    """Issue #118: the extra type information's content is a string after the
    type name (root-io-spec ERRATA 9), in the footer's schema extension (ERRATA 10)

    rntuple/streamed.root's case.toml pins the content length 438 at offset 1236,
    the streamed object's first bytes at 1240 (byte count 434 with the 0x40000000
    flag, then the new-class tag and "TList"), and the length byte 15 of the
    TStreamerInfo name "RNStreamedInner" at 1319.
    """
    path = DATA / "streamed.root"
    raw = path.read_bytes()
    with open_path(path) as reader:
        keylist = reader.keylist()
        (name,) = [n for n in keylist if keylist[n].fClassName == b"ROOT::RNTuple"]
        rntuple = RNTuple.from_anchor(reader.fetch(keylist[name]), reader.fetch.buffer)

    assert rntuple.headerEnvelope.extraTypeInformations.items == []
    (info,) = rntuple.footerEnvelope.schemaExtension.extraTypeInformations.items
    assert info.fContentIdentifier == 0
    assert info.fTypeVersion == 0
    assert info.fTypeName == b""
    assert type(info.fContent) is bytes
    assert int.from_bytes(raw[1236:1240], "little") == 438
    assert info.fContent == raw[1240 : 1240 + 438]
    assert info.fContent[:8] == bytes.fromhex("400001b2ffffffff")
    assert info.fContent[8:13] == b"TList"
    assert info.fContent[1319 - 1240] == 15
    assert info.fContent[1320 - 1240 : 1320 - 1240 + 15] == b"RNStreamedInner"
    # 8 (size) + 4 (content ID) + 4 (type version) + 4 + 0 (type name) + 4 + 438
    assert info.fSize == 462
    assert info._unknown == b""
