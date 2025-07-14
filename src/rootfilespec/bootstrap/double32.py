import dataclasses

from rootfilespec.buffer import ReadBuffer
from rootfilespec.serializable import Members, MemberSerDe


@dataclasses.dataclass
class Double32Reader:
    fname: str
    factor: float
    xmin: float
    xmax: float
    nbits: int

    def __call__(
        self, members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        if self.xmin == 0.0 and self.factor == 1.0 and self.nbits == 32:
            # read asfloat
            (val,), buffer = buffer.unpack(">f")
        else:
            # read as integer and scale
            nbytes = (self.nbits + 7) // 8
            fmt = "BHIQ"[(nbytes - 1) // 2]
            (raw,), buffer = buffer.unpack(f">{fmt}")
            val = min(self.xmin + self.factor * raw, self.xmax)

        members[self.fname] = float(val)
        return members, buffer


@dataclasses.dataclass
class Double32Serde(MemberSerDe):
    factor: float
    xmin: float
    xmax: float
    nbits: int

    def build_reader(self, fname: str, itype: type) -> Double32Reader:
        # itype not in use due to this reader being used only for Double32_t
        # will be in use when we support other types, e.g. float16
        if itype is not float:
            msg = f"Double32Serde.build_reader expected type float, got {itype}"
            raise ValueError(msg)

        return Double32Reader(fname, self.factor, self.xmin, self.xmax, self.nbits)


def parse_double32_title(title: str):
    """
    Very basic parser for ROOT Double32_t-style titles: '[xmin,xmax,nbits] title'
    Returns (xmin, xmax, nbits, factor), or (0, 0, 32, 1) if parsing fails.
    """
    title = title.strip()
    # filter out floats - unspecified xmin, xmax, nbits - from double32s
    bracket_end = title.find("]")
    if bracket_end == -1 or not title.startswith("["):
        return 0.0, 0.0, 32, 1.0
    tuple = title[1:bracket_end]
    params = [p.strip() for p in tuple.split(",")]

    # Parse coordinates
    if len(params) == 2:
        xmin, xmax = params
        nbits = "32"
    elif len(params) == 3:
        xmin, xmax, nbits = params
    else:
        msg = "expected 2 or 3 params in title"
        raise ValueError(msg)

    xmin_f = float(xmin.replace("pi", "3.141592653589793"))
    xmax_f = float(xmax.replace("pi", "3.141592653589793"))
    nbits_f = int(nbits)
    factor = (xmax_f - xmin_f) / (2**nbits_f - 1) if xmax_f != xmin_f else 1.0

    return xmin_f, xmax_f, nbits_f, factor
