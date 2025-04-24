from __future__ import annotations

from rootfilespec.dispatch import DICTIONARY
from rootfilespec.structutil import ReadBuffer, ROOTSerializable, serializable


@serializable
class TString(ROOTSerializable):
    """A class representing a TString."""

    fString: bytes
    """The string data."""

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        """Reads a TString from the given buffer.
        TStrings are always prefixed with a byte indicating the length of the string.
        If that byte is larger than 255, then there are 4 additional bytes are used to store the length.

        In ROOT, this is implemented at TBufferFile::ReadTString()
        https://root.cern/doc/v636/TBufferFile_8cxx_source.html#l00187
        """
        (length,), buffer = buffer.unpack(">B")
        if length == 255:
            (length,), buffer = buffer.unpack(">i")
        data, buffer = buffer.consume(length)
        return (data,), buffer


DICTIONARY["TString"] = TString
