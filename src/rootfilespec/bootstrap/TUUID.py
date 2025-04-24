from uuid import UUID

from rootfilespec.serializable import serializable
from rootfilespec.structutil import ReadBuffer, ROOTSerializable


@serializable
class TUUID(ROOTSerializable):
    fVersion: int
    fUUID: UUID

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        (fVersion,), buffer = buffer.unpack(">h")
        data, buffer = buffer.consume(16)
        uuid = UUID(bytes=data)
        return (fVersion, uuid), buffer
