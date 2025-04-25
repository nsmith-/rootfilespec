from typing import Annotated

import numpy as np

from rootfilespec.container import BasicArray
from rootfilespec.dispatch import DICTIONARY
from rootfilespec.serializable import ROOTSerializable, serializable
from rootfilespec.structutil import Fmt


@serializable
class TArray(ROOTSerializable):
    """A class to hold an array of a given type.

    Most popularly used in TH1x histograms.
    """

    # TODO: when ROOTSerializable calls base constructors this can be used
    # fN: Annotated[int, Fmt(">i")]


DICTIONARY["TArray"] = TArray


@serializable
class TArrayC(TArray):
    fN: Annotated[int, Fmt(">i")]
    fArray: Annotated[np.typing.NDArray[np.uint8], BasicArray(">B", "fN", haspad=False)]


DICTIONARY["TArrayC"] = TArrayC


@serializable
class TArrayS(TArray):
    fN: Annotated[int, Fmt(">i")]
    fArray: Annotated[np.typing.NDArray[np.short], BasicArray(">h", "fN", haspad=False)]


DICTIONARY["TArrayS"] = TArrayS


@serializable
class TArrayI(TArray):
    fN: Annotated[int, Fmt(">i")]
    fArray: Annotated[np.typing.NDArray[np.int32], BasicArray(">i", "fN", haspad=False)]


DICTIONARY["TArrayI"] = TArrayI


@serializable
class TArrayF(TArray):
    fN: Annotated[int, Fmt(">i")]
    fArray: Annotated[
        np.typing.NDArray[np.float32], BasicArray(">f", "fN", haspad=False)
    ]


DICTIONARY["TArrayF"] = TArrayF


@serializable
class TArrayD(TArray):
    fN: Annotated[int, Fmt(">i")]
    fArray: Annotated[
        np.typing.NDArray[np.float64], BasicArray(">d", "fN", haspad=False)
    ]


DICTIONARY["TArrayD"] = TArrayD
