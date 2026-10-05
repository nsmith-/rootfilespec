from pathlib import Path
from typing import get_type_hints

import pytest

from rootfilespec.bootstrap import BOOTSTRAP_CONTEXT, TNamed, TString
from rootfilespec.bootstrap.strings import TStringLong
from rootfilespec.bootstrap.TDatime import TDatime
from rootfilespec.bootstrap.TKey import TKey
from rootfilespec.dynamic import DynamicFileContext
from rootfilespec.reader import open_path
from rootfilespec.serializable import FileContext, ROOTSerializable, read_value

DATA = Path(__file__).parent.parent / "reference" / "root-io-spec" / "data"
UNFRAMED = DATA / "serialization" / "unframed-records.root"


@pytest.mark.parametrize(
    "method",
    [
        FileContext.type_by_name,
        type(BOOTSTRAP_CONTEXT).type_by_name,
        DynamicFileContext.type_by_name,
        TKey.read_from,
    ],
)
def test_return_type_is_object(method):
    """Issue #135: a lookup can return an annotated builtin, not only a class,
    and a key can read as a builtin, so neither is typed as ROOTSerializable"""
    assert get_type_hints(method)["return"] is object


@pytest.mark.skipif(not UNFRAMED.exists(), reason="root-io-spec not checked out")
def test_lookup_of_tdatime_is_an_annotated_builtin():
    """root-io-spec serialization/unframed-records stores a TDatime as a record
    of its own; its class name resolves to the annotated alias, which read_value
    reads and which has no read method. The case.toml pins the payload at 318:
    0x7E6CC000, 2026-09-22 12:00:00 packed"""
    with open_path(UNFRAMED) as reader:
        (key,) = [k for k in reader.keylist().values() if k.fClassName == b"TDatime"]
        buffer = reader.fetch.buffer(key)
        readtype = buffer.file_context.type_by_name("TDatime")
        assert readtype is TDatime
        assert not isinstance(readtype, type)
        payload = buffer[key.header.fKeylen :]
        value, rest = read_value(readtype, payload)
        assert value == 0x7E6CC000
        assert not rest
        # mypy rejects this call, which is what #135 asks for
        with pytest.raises(AttributeError):
            readtype.read(payload)  # type: ignore[attr-defined]
        assert reader.fetch(key) == 0x7E6CC000


@pytest.mark.skipif(not UNFRAMED.exists(), reason="root-io-spec not checked out")
@pytest.mark.parametrize(
    ("name", "expected"),
    [("TString", TString), ("TStringLong", TStringLong), ("string", TString)],
)
def test_lookup_of_strings(name: str, expected: object):
    """The string classes resolve to their annotated aliases (#68), in the
    bootstrap context and in a file's"""
    with open_path(UNFRAMED) as reader:
        key = next(iter(reader.keylist().values()))
        context = reader.fetch.buffer(key).file_context
    for ctx in (BOOTSTRAP_CONTEXT, context):
        assert ctx.type_by_name(name) is expected


def test_lookup_of_a_class_narrows():
    """A caller that needs the class narrows the result"""
    readtype = BOOTSTRAP_CONTEXT.type_by_name("TNamed")
    assert isinstance(readtype, type)
    assert issubclass(readtype, ROOTSerializable)
    assert readtype is TNamed
