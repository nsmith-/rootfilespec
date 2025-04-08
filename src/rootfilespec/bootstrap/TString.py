from __future__ import annotations

from rootfilespec.dispatch import DICTIONARY
from rootfilespec.structutil import ReadBuffer, ROOTSerializable, serializable


@serializable
class TString(ROOTSerializable):
    """A class representing a TString.
    """

    fString: bytes

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        """Reads a TString from the given buffer.
        TStrings are always prefixed with a byte indicating the length of the string.
        """
        (length,), buffer = buffer.unpack(">B")
        if length == 255:
            (length,), buffer = buffer.unpack(">I")
        data, buffer = buffer.consume(length)
        return (data,), buffer


DICTIONARY["TString"] = TString
