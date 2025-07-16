from enum import IntEnum
from typing import Annotated, Optional

from rootfilespec.bootstrap.strings import RString
from rootfilespec.rntuple.RFrame import RecordFrame
from rootfilespec.serializable import serializable
from rootfilespec.structutil import Fmt, OptionalField


class ColumnType(IntEnum):
    """The type of the column.

    The "split encoding" columns apply a byte transformation encoding to all pages of
    that column and in addition, depending on the column type, delta or zigzag encoding:

    - Split (only) : Rearranges the bytes of elements: All the first bytes first, then all the second bytes, etc.
    - Delta + split : The first element is stored unmodified, all other elements store the delta to the previous element.
        Followed by split encoding.
    - Zigzag + split : Used on signed integers only; it maps `x` to `2x` if `x` is positive and to `-(2x+1)` if `x` is negative.
        Followed by split encoding.
    Note: these encodings always happen within each page, thus decoding should be done page-wise, not cluster-wise.

    The Real32Trunc type column is a variable-sized floating point column with lower precision than Real32 and SplitReal32.
        It is an IEEE-754 single precision float with some of the mantissa's least significant bits truncated.
    The Real32Quant type column is a variable-sized real column that is internally represented as an integer within
        a specified range of values. For this column type, flag 0x02 (column with range) is always set.
    Future versions of the file format may introduce additional column types without changing the minimum version
        of the header or introducing a feature flag. Old readers need to ignore these columns and fields constructed
        from such columns. Old readers can, however, figure out the number of elements stored in such unknown columns."""

    kBit = 0x00
    "Boolean value"
    kByte = 0x01
    "An uninterpreted byte, e.g. part of a blob"
    kChar = 0x02
    "ASCII character"
    kInt8 = 0x03
    "Two's complement, 1-byte signed integer"
    kUInt8 = 0x04
    "1 byte unsigned integer"
    kInt16 = 0x05
    "Two's complement, little-endian 2-byte signed integer"
    kUInt16 = 0x06
    "Little-endian 2-byte unsigned integer"
    kInt32 = 0x07
    "Two's complement, little-endian 4-byte signed integer"
    kUInt32 = 0x08
    "Little-endian 4-byte unsigned integer"
    kInt64 = 0x09
    "Two's complement, little-endian 8-byte signed integer"
    kUInt64 = 0x0A
    "Little-endian 8-byte unsigned integer"
    kReal16 = 0x0B
    "IEEE-754 half precision float"
    kReal32 = 0x0C
    "IEEE-754 single precision float"
    kReal64 = 0x0D
    "IEEE-754 double precision float"
    kIndex32 = 0x0E
    "Parent columns of (nested) collections, counting is relative to the cluster"
    kIndex64 = 0x0F
    "Parent columns of (nested) collections, counting is relative to the cluster"
    kSwitch = 0x10
    "Tuple of a kIndex64 value followed by a 32 bits dispatch tag to a column ID"
    kSplitInt16 = 0x11
    "Like Int16 but in split + zigzag encoding"
    kSplitUInt16 = 0x12
    "Like UInt16 but in split encoding"
    kSplitInt32 = 0x13
    "Like Int32 but in split + zigzag encoding"
    kSplitUInt32 = 0x14
    "Like UInt32 but in split encoding"
    kSplitInt64 = 0x15
    "Like Int64 but in split + zigzag encoding"
    kSplitUInt64 = 0x16
    "Like UInt64 but in split encoding"
    kSplitReal16 = 0x17
    "Like Real16 but in split encoding"
    kSplitReal32 = 0x18
    "Like Real32 but in split encoding"
    kSplitReal64 = 0x19
    "Like Real64 but in split encoding"
    kSplitIndex32 = 0x1A
    "Like Index32 but pages are stored in split + delta encoding"
    kSplitIndex64 = 0x1B
    "Like Index64 but pages are stored in split + delta encoding"
    kReal32Trunc = 0x1C
    "IEEE-754 single precision float with truncated mantissa"
    kReal32Quant = 0x1D
    "Real value contained in a specified range with an underlying quantized integer representation"

    def __repr__(self) -> str:
        """Get a string representation of this element type."""
        return f"{self.__class__.__name__}.{self.name}"


@serializable
class FieldDescription(RecordFrame):
    """A class representing an RNTuple Field Description Record Frame.
    This Record Frame is found in the Header Envelope of an RNTuple and can be extended in the Footer Envelope.
    It describes a field in the RNTuple schema."""

    fFieldVersion: Annotated[int, Fmt("<I")]
    """The version of the field. Used for schema evolution."""
    fTypeVersion: Annotated[int, Fmt("<I")]
    """The version of the field type. Used for schema evolution."""
    fParentFieldID: Annotated[int, Fmt("<I")]
    """The ID of the parent field, if this field is a sub-field.
        Top-level fields have their own field ID set as parent ID."""
    fStructuralRole: Annotated[int, Fmt("<H")]
    """The structural role of the field; can have one of the following values:
        - Value: Meaning
        - 0x00:  Leaf field in the schema tree
        - 0x01:  The field is the parent of a collection (e.g., a vector)
        - 0x02:  The field is the parent of a record (e.g., a struct)
        - 0x03:  The field is the parent of a variant
        - 0x04:  The field stores objects serialized with the ROOT streamer"""
    fFlags: Annotated[int, Fmt("<H")]
    """The flags for the field; can have any of the following bits set:
        - Bit:   Meaning
        - 0x01:  Repetitive field, i.e. for every entry n copies of the field are stored
        - 0x02:  Projected field
        - 0x04:  Has ROOT type checksum as reported by TClass"""
    fFieldName: RString
    """The name of the field."""
    fTypeName: RString
    """The name of the field type."""
    fTypeAlias: RString
    """The alias of the field type, if any."""
    fFieldDescription: RString
    """The description of the field, if any."""
    fArraySize: Annotated[Optional[int], OptionalField("<Q", "fFlags", "&", 0x01)]
    """The size of the array for the field. Present only if flag 0x01 is set (repetitive field)."""
    fSourceFieldID: Annotated[Optional[int], OptionalField("<I", "fFlags", "&", 0x02)]
    """The ID of the source field. Present only if flag 0x02 is set (projected field)."""
    fTypeChecksum: Annotated[Optional[int], OptionalField("<I", "fFlags", "&", 0x04)]
    """The ROOT type checksum for the field. Present only if flag 0x04 is set (has ROOT type checksum)."""


@serializable
class ColumnDescription(RecordFrame):
    """A class representing an RNTuple Column Description Record Frame.
    This Record Frame is found in the Header Envelope of an RNTuple and can be extended in the Footer Envelope.
    It describes a column in the RNTuple schema."""

    """ abbott TODO: read this when not sick and understand it
    Future versions of the file format may introduce additional column types without
    changing the minimum version of the header or introducing a feature flag.
    Old readers need to ignore these columns and fields constructed from such columns.
    Old readers can, however, figure out the number of elements stored in such unknown columns.
    """

    fColumnType: Annotated[ColumnType, Fmt("<H")]
    """The type of the column."""
    fBitsOnStorage: Annotated[int, Fmt("<H")]
    """The number of bits used to store the column value."""
    fFieldID: Annotated[int, Fmt("<I")]
    """The ID of the field that this column belongs to.
    The field ID is the zero-based index of the field in the serialized list of field descriptions in the Header Envelope."""
    fFlags: Annotated[int, Fmt("<H")]
    """The flags for the column; can have any of the following bits set:
        - Bit:   Meaning
        - 0x01:  Deferred column: index of first element in the column is not zero
        - 0x02:  Column with a range of possible values"""
    fRepresentationIndex: Annotated[int, Fmt("<H")]
    """The index of the representation of the column in the list of representations for the field."""
    # abbott TODO: verify the below are signed. make PR updating ROOT documentation if so (indicate signed bit in table)
    fFirstElementIndex: Annotated[
        Optional[int], OptionalField("<q", "fFlags", "&", 0x01)
    ]
    """The index of the first element in the column. Present only if flag 0x01 is set (deferred column)."""
    fMinValue: Annotated[Optional[int], OptionalField("<q", "fFlags", "&", 0x02)]
    """The minimum value of the column. Present only if flag 0x02 is set (column with range of values)."""
    fMaxValue: Annotated[Optional[int], OptionalField("<q", "fFlags", "&", 0x02)]
    """The maximum value of the column. Present only if flag 0x02 is set (column with range of values)."""


@serializable
class AliasColumnDescription(RecordFrame):
    """A class representing an RNTuple Alias Column Description Record Frame.
    This Record Frame is found in the Header Envelope of an RNTuple and can be extended in the Footer Envelope.
    It describes an alias column in the RNTuple schema."""

    fPhysicalColumnID: Annotated[int, Fmt("<I")]
    """The physical column ID of the alias column."""
    fFieldID: Annotated[int, Fmt("<I")]
    """The ID of the field that this alias column belongs to."""


@serializable
class ExtraTypeInformation(RecordFrame):
    """A class representing an RNTuple Extra Type Information Record Frame.
    This Record Frame is found in the Header Envelope of an RNTuple and can be extended in the Footer Envelope.
    It provides additional type information for the RNTuple schema for certain field types."""

    fContentIdentifier: Annotated[int, Fmt("<I")]
    """The content identifier for the extra type information.

    - Content identifier:  Meaning of content
    - 0x00:	              Serialized ROOT streamer info

    The serialized ROOT streamer info is not bound to a specific type.
    It is the combined streamer information from all fields serialized by the ROOT streamer.
    Writers set the version to zero and use an empty type name. Readers should ignore the type-specific information.
    The format of the content is a ROOT streamed TList of TStreamerInfo objects."""
    fTypeVersion: Annotated[int, Fmt("<I")]
    """The version of the type for which this extra type information is provided."""
    fTypeName: RString
    """The name of the type for which this extra type information is provided."""
