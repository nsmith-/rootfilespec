from typing import Annotated, Optional

import numpy as np

from rootfilespec.bootstrap import *
from rootfilespec.container import (
    BasicArray,
    FixedSizeArray,
    ObjectArray,
    StdDeque,
    StdMap,
    StdPair,
    StdSet,
    StdVector,
)
from rootfilespec.serializable import ROOTSerializable, serializable
from rootfilespec.structutil import Fmt, StdBitset
