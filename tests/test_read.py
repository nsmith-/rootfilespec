import sys
import types
from pathlib import Path

import pytest
from skhep_testdata import data_path, known_files  # type: ignore[import-not-found]

from rootfilespec.bootstrap import ROOTFile, TDirectory
from rootfilespec.buffer import DataFetcher, ReadBuffer
from rootfilespec.dispatch import DICTIONARY
from rootfilespec.dynamic import streamerinfo_to_classes

TESTABLE_FILES = [f for f in known_files if f.endswith(".root")]


def _walk(dir: TDirectory, fetch_data: DataFetcher, depth=0, maxdepth=-1):
    keylist = dir.get_KeyList(fetch_data)
    for item in keylist.values():
        obj = item.read_object(fetch_data)
        if isinstance(obj, TDirectory) and (maxdepth < 0 or depth < maxdepth):
            _walk(obj, fetch_data, depth + 1)


@pytest.mark.parametrize("filename", TESTABLE_FILES)
def test_read_file(filename: str):
    initial_read_size = 512
    path = Path(data_path(filename))
    with path.open("rb") as filehandle:

        def fetch_data(seek: int, size: int):
            filehandle.seek(seek)
            return ReadBuffer(memoryview(filehandle.read(size)), seek, 0)

        buffer = fetch_data(0, initial_read_size)
        file, _ = ROOTFile.read(buffer)

        # Read root directory object, which should be contained in the initial buffer
        def fetch_cached(seek: int, size: int):
            if seek + size <= len(buffer):
                return buffer[seek : seek + size]
            msg = "Didn't find data in initial read buffer"
            raise ValueError(msg)

        rootdir = file.get_TFile(fetch_cached).rootdir

        # Read all StreamerInfo (class definitions) from the file
        streamerinfo = file.get_StreamerInfo(fetch_data)
        if not streamerinfo:
            # Try to read all objects anyway
            _walk(rootdir, fetch_data)
            return None

        # Render the class definitions into python code
        try:
            classes = streamerinfo_to_classes(streamerinfo)
        except NotImplementedError as ex:
            return pytest.xfail(reason=str(ex))

        # Evaluate the python code to create the classes and add them to the DICTIONARY
        oldkeys = set(DICTIONARY)
        try:
            module = types.ModuleType(f"rootfilespec.generated_{id(streamerinfo)}")
            sys.modules[module.__name__] = module
            exec(classes, module.__dict__)

            # Read all objects from the file
            return _walk(rootdir, fetch_data)
        except NotImplementedError as ex:
            return pytest.xfail(reason=str(ex))
        finally:
            for key in set(DICTIONARY) - oldkeys:
                DICTIONARY.pop(key)
