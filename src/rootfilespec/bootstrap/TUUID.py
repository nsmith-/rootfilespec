from uuid import UUID

from rootfilespec.buffer import ReadBuffer
from rootfilespec.serializable import Members, ROOTSerializable, serializable


@serializable
class TUUID(ROOTSerializable):
    fVersion: int
    fUUID: UUID

    @classmethod
    def update_members(cls, members: Members, buffer: ReadBuffer):
        (fVersion,), buffer = buffer.unpack(">h")
        data, buffer = buffer.consume(16)
        uuid = UUID(bytes=data)
        members["fVersion"] = fVersion
        members["fUUID"] = uuid
        return members, buffer
