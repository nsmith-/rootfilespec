"""Minimal set of types found in TFile-like ROOT files

With these, we can read the self-describing part of the file, namely the
TStreamerInfo dictionary of types, along with the directory structure and
object references (TKey and TBasket)

These types generally hold big-endian encoded primitive types.
"""

import dataclasses

from rootfilespec.bootstrap.array import (
    TArray,
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
from rootfilespec.bootstrap.strings import RString, STLString, TString, string
from rootfilespec.bootstrap.TBasket import TBasket
from rootfilespec.bootstrap.TDatime import TDatime
from rootfilespec.bootstrap.TDirectory import TDirectory, TDirectoryFile, TKeyList
from rootfilespec.bootstrap.TFile import ROOTFile, TFile
from rootfilespec.bootstrap.TKey import TKey
from rootfilespec.bootstrap.TList import TCollection, TList, TObjArray, TSeqCollection
from rootfilespec.bootstrap.TObject import TNamed, TObject, TObjString
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
from rootfilespec.serializable import FileContext, ROOTSerializable


@dataclasses.dataclass
class _BootstrapContext(FileContext):
    types: dict[str, type[ROOTSerializable]]

    def type_by_name(
        self, name: str, expect_version: int | None = None
    ) -> type[ROOTSerializable]:
        cls = self.types.get(name)
        if cls is None:
            msg = f"Cannot find type {name} (expected version {expect_version})"
            raise KeyError(msg)
        return cls


BOOTSTRAP_CONTEXT = _BootstrapContext(
    types={
        name: cls
        for name, cls in globals().items()
        if isinstance(cls, type) and issubclass(cls, ROOTSerializable)
    }
)

__all__ = [
    "BOOTSTRAP_CONTEXT",
    "Double32Serde",
    "RCompressed",
    "RCompressionHeader",
    "ROOT3a3aRNTuple",
    "ROOTFile",
    "RString",
    "Ref",
    "RooLinkedList",
    "STLString",
    "StreamedObject",
    "TArray",
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
    "TDirectoryFile",
    "TFile",
    "TFormula",
    "TKey",
    "TKeyList",
    "TList",
    "TNamed",
    "TObjArray",
    "TObjString",
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
