"""Minimal set of types found in TFile-like ROOT files

With these, we can read the self-describing part of the file, namely the
TStreamerInfo dictionary of types, along with the directory structure and
object references (TKey and TBasket)

These types generally hold big-endian encoded primitive types.
"""

from rootfilespec.bootstrap.array import (
    TArrayC,
    TArrayD,
    TArrayF,
    TArrayI,
    TArrayS,
)
from rootfilespec.bootstrap.assumed import (
    RooLinkedList,
    TAtt3D,
    TFormula,
    TVirtualIndex,
    Uninterpreted,
)
from rootfilespec.bootstrap.compression import RCompressed, RCompressionHeader
from rootfilespec.bootstrap.double32 import Double32Serde
from rootfilespec.bootstrap.RAnchor import ROOT3a3aRNTuple
from rootfilespec.bootstrap.streamedobject import Ref, StreamedObject
from rootfilespec.bootstrap.strings import STLString, TString, string
from rootfilespec.bootstrap.TBasket import TBasket
from rootfilespec.bootstrap.TDatime import TDatime
from rootfilespec.bootstrap.TDirectory import TDirectory, TKeyList
from rootfilespec.bootstrap.TFile import ROOTFile, TFile
from rootfilespec.bootstrap.TKey import TKey
from rootfilespec.bootstrap.TList import TCollection, TList, TObjArray, TSeqCollection
from rootfilespec.bootstrap.TObject import TNamed, TObject
from rootfilespec.bootstrap.TStreamerInfo import (
    TStreamerBase,
    TStreamerBasicPointer,
    TStreamerBasicType,
    TStreamerElement,
    TStreamerInfo,
    TStreamerLoop,
    TStreamerObject,
    TStreamerObjectAny,
    TStreamerObjectAnyPointer,
    TStreamerObjectPointer,
    TStreamerSTL,
    TStreamerSTLstring,
    TStreamerString,
)

__all__ = [
    "Double32Serde",
    "RCompressed",
    "RCompressionHeader",
    "ROOT3a3aRNTuple",
    "ROOTFile",
    "Ref",
    "RooLinkedList",
    "STLString",
    "StreamedObject",
    "TArrayC",
    "TArrayD",
    "TArrayF",
    "TArrayI",
    "TArrayS",
    "TAtt3D",
    "TBasket",
    "TCollection",
    "TDatime",
    "TDirectory",
    "TFile",
    "TFormula",
    "TKey",
    "TKeyList",
    "TList",
    "TNamed",
    "TObjArray",
    "TObject",
    "TSeqCollection",
    "TStreamerBase",
    "TStreamerBasicPointer",
    "TStreamerBasicType",
    "TStreamerElement",
    "TStreamerInfo",
    "TStreamerLoop",
    "TStreamerObject",
    "TStreamerObjectAny",
    "TStreamerObjectAnyPointer",
    "TStreamerObjectPointer",
    "TStreamerSTL",
    "TStreamerSTLstring",
    "TStreamerString",
    "TString",
    "TVirtualIndex",
    "Uninterpreted",
    "string",
]
