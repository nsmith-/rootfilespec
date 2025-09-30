from rootfilespec.bootstrap.streamedobject import StreamedObject
from rootfilespec.buffer import ReadBuffer
from rootfilespec.dispatch import DICTIONARY
from rootfilespec.serializable import Members, ROOTSerializable, serializable
from rootfilespec.serializable import MemberSerDe

from typing import Optional
import dataclasses

@dataclasses.dataclass
class Double32Reader:
    fname: str
    factor: Optional[float]
    xmin: Optional[float]
    nbits: Optional[int]

    def __call__(self, members: Members, buffer: ReadBuffer) -> tuple[Members, ReadBuffer]:
        # Float32 Read
        (val,), buffer = buffer.unpack(">f")
        members[self.fname] = float(val)
        return members, buffer
    
@dataclasses.dataclass
class Double32Serde(MemberSerDe):
    factor: Optional[float] = None
    xmin: Optional[float] = None
    nbits: Optional[int] = None

    def build_reader(self, fname: str, ftype: type):
        return Double32Reader(fname, self.factor, self.xmin, self.nbits)