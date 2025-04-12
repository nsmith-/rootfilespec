from pathlib import Path

import pytest
from skhep_testdata import data_path, known_files  # type: ignore[import-not-found]

from rootfilespec.bootstrap import ROOTFile, TDirectory
from rootfilespec.dynamic import streamerinfo_to_classes
from rootfilespec.structutil import DataFetcher, ReadBuffer

TESTABLE_FILES = [f for f in known_files if f.endswith(".root")]


def _walk(dir: TDirectory, fetch_data: DataFetcher, depth=0):
    keylist = dir.get_KeyList(fetch_data)
    for item in keylist.values():
        obj = item.read_object(fetch_data)
        if isinstance(obj, TDirectory):
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

        def fetch_cached(seek: int, size: int):
            if seek + size <= len(buffer):
                return buffer[seek : seek + size]
            msg = "Didn't find data in initial read buffer"
            raise ValueError(msg)

        # oldkeys = set(DICTIONARY)
        streamerinfo = file.get_StreamerInfo(fetch_data)

        if streamerinfo:
            try:
                streamerinfo_to_classes(streamerinfo)
                # exec(classes, globals())
            except NotImplementedError as ex:
                pytest.xfail(reason=str(ex))

        # newkeys = set(DICTIONARY) - oldkeys
        # tfile = file.get_TFile(fetch_cached)
        # rootdir = tfile.rootdir
        # keylist = rootdir.get_KeyList(fetch_data)
        # for item in keylist.values():
        #     _ = item.read_object(fetch_data)

        # for key in newkeys:
        #     DICTIONARY.pop(key)
