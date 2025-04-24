"""Minimal set of types found in TFile-like ROOT files

With these, we can read the self-describing part of the file, namely the
TStreamerInfo dictionary of types, along with the directory structure and
object references (TKey and TBasket)

These types generally hold big-endian encoded primitive types.
"""

from rootfilespec.bootstrap.assumed import ROOT3a3aTIOFeatures
from rootfilespec.bootstrap.RAnchor import ROOT3a3aRNTuple
from rootfilespec.bootstrap.TDirectory import TDirectory, TKeyList
from rootfilespec.bootstrap.TFile import ROOTFile, TFile
from rootfilespec.bootstrap.TKey import TKey
from rootfilespec.bootstrap.TList import TList, TObjArray
from rootfilespec.bootstrap.TObject import StreamedObject, TNamed, TObject
from rootfilespec.bootstrap.TStreamerInfo import (
    TStreamerBase,
    TStreamerElement,
    TStreamerInfo,
    TStreamerString,
)
from rootfilespec.bootstrap.TString import TString, string

__all__ = [
    "ROOT3a3aRNTuple",
    "ROOT3a3aTIOFeatures",
    "ROOTFile",
    "StreamedObject",
    "TDirectory",
    "TFile",
    "TKey",
    "TKeyList",
    "TList",
    "TNamed",
    "TObjArray",
    "TObject",
    "TStreamerBase",
    "TStreamerElement",
    "TStreamerInfo",
    "TStreamerString",
    "TString",
    "string",
]
