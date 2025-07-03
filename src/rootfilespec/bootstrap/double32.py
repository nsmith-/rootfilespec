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
    factor: float
    xmin: float
    nbits: int

    def __call__(self, members: Members, buffer: ReadBuffer) -> tuple[Members, ReadBuffer]:
        if self.factor is None or self.xmin is None or self.nbits is None:
            # Uncompressed: read as float32
            (val,), buffer = buffer.unpack(">f")
            members[self.fname] = float(val)
        else:
            # read as unsigned int and apply scaling
            nbytes = (self.nbits + 7) // 8
            (raw,), buffer = buffer.unpack(f">{'BHIQ'[(nbytes-1)//2]}")
            members[self.fname] = float(self.xmin + self.factor * raw)
        
        return members, buffer
    
@dataclasses.dataclass
class Double32Serde:
    def __init__(self, factor, xmin, xmax, nbits):
        self.factor = factor
        self.xmin = xmin
        self.xmax = xmax
        self.nbits = nbits

    def build_reader(self, fname: str, ftype: type):
        return Double32Reader(fname, self.factor, self.xmin, self.nbits)

    def __repr__(self):
        return (
            f"Double32Serde(factor={self.factor}, "
            f"xmin={self.xmin}, xmax={self.xmax}, "
            f"nbits={self.nbits})"
        )

DICTIONARY["Double32_t"] = Double32Serde(factor=1.0, xmin=0.0, xmax=0.0, nbits=32)
DICTIONARY["Double32Serde"] = Double32Serde

def parse_double32_title(title: str):
    """
    Very basic parser for ROOT Double32_t-style titles: '[xmin,xmax,nbits]'.
    Returns a tuple: (xmin, xmax, nbits), or (None, None, None) if parsing fails.
    """
    title = title.strip()
    
    if not (title.startswith("[") and title.endswith("]")):
        return None, None, None

    content = title[1:-1]  # strip off the brackets
    parts = content.split(",")
    
    if len(parts) != 3:
        return None, None, None

    try:
        xmin = float(parts[0].strip()) if parts[0].strip() else None
        xmax = float(parts[1].strip()) if parts[1].strip() else None
        nbits = int(parts[2].strip())
        return xmin, xmax, nbits
    except Exception:
        return 0.0, 0.0, 32  # Return safe defaults on any error