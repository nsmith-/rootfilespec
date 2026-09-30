from pathlib import Path

import pytest

from rootfilespec.reader import open_path
from rootfilespec.rntuple.RNTuple import RNTuple

DATA = Path(__file__).parent.parent / "reference" / "root-io-spec" / "data" / "rntuple"


@pytest.mark.skipif(not DATA.exists(), reason="reference/root-io-spec not checked out")
def test_streamer_info_content():
    """Issue #118: the extra type information's content is a string after the
    type name (root-io-spec ERRATA 9), in the footer's schema extension (ERRATA 10)

    rntuple/streamed.root's case.toml asserts the content length 438 and the
    streamed object's first bytes: byte count 434 with the 0x40000000 flag, the
    new-class tag, then "TList".
    """
    path = DATA / "streamed.root"
    with open_path(path) as reader:
        keylist = reader.keylist()
        (name,) = [
            n for n in keylist if keylist[n].fClassName.fString == b"ROOT::RNTuple"
        ]
        rntuple = RNTuple.from_anchor(reader.fetch(keylist[name]), reader.fetch.buffer)

    assert rntuple.headerEnvelope.extraTypeInformations.items == []
    (info,) = rntuple.footerEnvelope.schemaExtension.extraTypeInformations.items
    assert info.fContentIdentifier == 0
    assert info.fTypeVersion == 0
    assert info.fTypeName == b""
    assert type(info.fContent) is bytes
    assert len(info.fContent) == 438
    assert info.fContent[:8] == bytes.fromhex("400001b2ffffffff")
    assert info.fContent[8:13] == b"TList"
    assert info._unknown == b""
    assert b"RNStreamedInner" in info.fContent
