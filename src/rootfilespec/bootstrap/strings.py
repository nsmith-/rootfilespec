from rootfilespec.bootstrap.streamedobject import StreamedObject
from rootfilespec.buffer import ReadBuffer
from rootfilespec.dispatch import DICTIONARY
from rootfilespec.serializable import Members, ROOTSerializable, serializable


@serializable
class TString(ROOTSerializable):
    """A class representing a TString.

    TODO: this can just be Annotated[bytes, TString] since nobody subclasses it
    """

    fString: bytes
    """The string data."""

    def __hash__(self) -> int:
        return hash(self.fString)

    @classmethod
    def update_members(cls, members: Members, buffer: ReadBuffer):
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
        members["fString"] = data
        return members, buffer


# No examples so far of TString being streamed but it is in most StreamerInfo
DICTIONARY["TString"] = TString

string = TString
DICTIONARY["string"] = TString


@serializable
class STLString(StreamedObject):
    """String with a stream header (see also TObjString)"""

    value: bytes

    @classmethod
    def update_members(cls, members: Members, buffer: ReadBuffer):
        (length,), buffer = buffer.unpack(">B")
        if length == 255:
            (length,), buffer = buffer.unpack(">i")
        data, buffer = buffer.consume(length)
        members["value"] = data
        return members, buffer


DICTIONARY["STLString"] = STLString


@serializable
class RString(ROOTSerializable):
    """A class representing an RString."""

    fString: bytes
    """The string data."""

    def __hash__(self) -> int:
        return hash(self.fString)

    @classmethod
    def update_members(cls, members: Members, buffer: ReadBuffer):
        """Reads an RString from the given buffer.
        RStrings are always prefixed with a 32bit unsigned integer indicating the length of the string.
        String data are UTF-8 encoded.
        """

        (length,), buffer = buffer.unpack("<I")
        data, buffer = buffer.consume(length)
        members["fString"] = data
        return members, buffer
