from rootfilespec.bootstrap.strings import RString
from rootfilespec.rntuple.envelope import ENVELOPE_TYPE_MAP, REnvelope, RFeatureFlags
from rootfilespec.rntuple.RFrame import ListFrame
from rootfilespec.rntuple.schema import (
    AliasColumnDescription,
    ColumnDescription,
    ExtraTypeInformation,
    FieldDescription,
)
from rootfilespec.serializable import serializable


@serializable
class HeaderEnvelope(REnvelope):
    """A class representing the RNTuple Header Envelope payload structure"""

    featureFlags: RFeatureFlags
    """The RNTuple Feature Flags (verify this file can be read)"""
    fName: RString
    """The name of the RNTuple."""
    fDescription: RString
    """The description of the RNTuple."""
    fLibrary: RString
    """The library or program used to create the RNTuple."""
    fieldDescriptions: ListFrame[FieldDescription]
    """The List Frame of Field Description Record Frames. Part of the RNTuple schema description.
    The order of fields matters: every field gets an implicit field ID which is equal the zero-based
    index of the field in the serialized list; subfields are ordered from smaller IDs to larger IDs.
    Top-level fields have their own field ID set as parent ID."""
    columnDescriptions: ListFrame[ColumnDescription]
    """The List Frame of Column Description Record Frames. Part of the RNTuple schema description."""
    aliasColumnDescriptions: ListFrame[AliasColumnDescription]
    """The List Frame of Alias Column Description Record Frames. Part of the RNTuple schema description."""
    extraTypeInformations: ListFrame[ExtraTypeInformation]
    """The List Frame of Extra Type Information Record Frames. Part of the RNTuple schema description."""


ENVELOPE_TYPE_MAP[0x01] = "HeaderEnvelope"
