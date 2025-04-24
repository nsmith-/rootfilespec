"""RNTuple class definitions

This module contains the class definitions for the RNTuple format.
The format schema is defined in the ROOT documentation:
https://root.cern/doc/v634/md_tree_2ntuple_2v7_2doc_2BinaryFormatSpecification.html

Generally, all primitives in this module are encoded little-endian.
"""

from rootfilespec.rntuple.footer import FooterEnvelope
from rootfilespec.rntuple.header import HeaderEnvelope
from rootfilespec.rntuple.pagelist import PageListEnvelope

__all__ = [
    "FooterEnvelope",
    "HeaderEnvelope",
    "PageListEnvelope",
]
