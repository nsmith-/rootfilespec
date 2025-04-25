from dataclasses import dataclass
from enum import IntEnum
from typing import Annotated

from rootfilespec.bootstrap.strings import TString
from rootfilespec.bootstrap.TList import TObjArray
from rootfilespec.bootstrap.TObject import TNamed
from rootfilespec.cpptype import cpptype_to_pytype
from rootfilespec.dispatch import DICTIONARY, normalize
from rootfilespec.serializable import serializable
from rootfilespec.structutil import Fmt


@dataclass
class ClassDef:
    name: str
    dependencies: list[str]
    code: str


@serializable
class TStreamerInfo(TNamed):
    fCheckSum: Annotated[int, Fmt(">i")]
    fClassVersion: Annotated[int, Fmt(">i")]
    fObjects: TObjArray

    def class_name(self) -> str:
        """Get the class name of this streamer info."""
        return normalize(self.fName.fString)

    def base_classes(self) -> list[str]:
        """Get the base classes of this streamer info."""
        bases: list[str] = []
        for element in self.fObjects.objects:
            if isinstance(element, TStreamerBase):
                bases.append(element.member_name())
        if not bases:
            bases.append("StreamedObject")
        return bases

    def class_definition(self) -> ClassDef:
        """Get the class definition code of this streamer info."""
        bases = self.base_classes()
        members: list[str] = []
        dependencies: list[str] = []
        for element in self.fObjects.objects:
            if not isinstance(element, (TStreamerElement, TStreamerSTLstring)):
                msg = f"Unexpected element: {element}"
                raise TypeError(msg)
            if isinstance(element, TStreamerBase):
                continue
            mdef, dep = element.member_definition(parent=self)
            dependencies.extend(dep)
            members.append(mdef)
        clsname = self.class_name()
        basestr = ", ".join(reversed(bases))
        lines: list[str] = []
        lines.append(f"# Generated for {self}")
        lines.append("@serializable")
        lines.append(f"class {clsname}({basestr}):")
        for member in members:
            lines.append("    " + member)
        if not members:
            lines.append("    pass")
        lines.append("\n")
        lines.append(f"DICTIONARY['{clsname}'] = {clsname}")
        lines.append("")
        return ClassDef(clsname, bases + dependencies, "\n".join(lines))


DICTIONARY["TStreamerInfo"] = TStreamerInfo


class ElementType(IntEnum):
    """Element type codes.

    See https://github.com/root-project/root/blob/v6-34-08/core/meta/inc/TVirtualStreamerInfo.h#L116-L140
    """

    kBase = 0
    "Base class element"
    kOffsetL = 20
    "Fixed size array"
    kOffsetP = 40
    "Pointer to object"
    kCounter = 6
    "Counter for array size"
    kCharStar = 7
    "Pointer to array of char"
    kChar = 1
    kShort = 2
    kInt = 3
    kLong = 4
    kFloat = 5
    kDouble = 8
    kDouble32 = 9
    kLegacyChar = 10
    "Equal to TDataType's kchar"
    kUChar = 11
    kUShort = 12
    kUInt = 13
    kULong = 14
    kBits = 15
    "TObject::fBits in case of a referenced object"
    kLong64 = 16
    kULong64 = 17
    kBool = 18
    kFloat16 = 19
    # Arrays of built-in types
    kArrayChar = kOffsetL + 1
    kArrayShort = kOffsetL + 2
    kArrayInt = kOffsetL + 3
    kArrayLong = kOffsetL + 4
    kArrayFloat = kOffsetL + 5
    kArrayDouble = kOffsetL + 8
    kArrayDouble32 = kOffsetL + 9
    kArrayLegacyChar = kOffsetL + 10
    kArrayUChar = kOffsetL + 11
    kArrayUShort = kOffsetL + 12
    kArrayUInt = kOffsetL + 13
    kArrayULong = kOffsetL + 14
    kArrayBits = kOffsetL + 15
    kArrayLong64 = kOffsetL + 16
    kArrayULong64 = kOffsetL + 17
    kArrayBool = kOffsetL + 18
    kArrayFloat16 = kOffsetL + 19
    # Pointers to built-in types
    kPointerChar = kOffsetP + 1
    kPointerShort = kOffsetP + 2
    kPointerInt = kOffsetP + 3
    kPointerLong = kOffsetP + 4
    kPointerFloat = kOffsetP + 5
    kPointerDouble = kOffsetP + 8
    kPointerDouble32 = kOffsetP + 9
    kPointerLegacyChar = kOffsetP + 10
    kPointerUChar = kOffsetP + 11
    kPointerUShort = kOffsetP + 12
    kPointerUInt = kOffsetP + 13
    kPointerULong = kOffsetP + 14
    kPointerBits = kOffsetP + 15
    kPointerLong64 = kOffsetP + 16
    kPointerULong64 = kOffsetP + 17
    kPointerBool = kOffsetP + 18
    kPointerFloat16 = kOffsetP + 19
    kObject = 61
    "Class derived from TObject, or for TStreamerSTL::fCtype non-pointer elements"
    kAny = 62
    kObjectp = 63
    "Class* derived from TObject and with    comment field //->Class, or for TStreamerSTL::fCtype: pointer elements"
    kObjectP = 64
    "Class* derived from TObject and with NO comment field //->Class"
    kTString = 65
    kTObject = 66
    kTNamed = 67
    kAnyp = 68
    kAnyP = 69
    kAnyPnoVT = 70
    kSTLp = 71
    kObjectL = kObject + kOffsetL
    kAnyL = kAny + kOffsetL
    kObjectpL = kObjectp + kOffsetL
    kObjectPL = kObjectP + kOffsetL
    kSkip = 100
    kSkipL = 120
    kSkipP = 140
    kConv = 200
    kConvL = 220
    kConvP = 240
    kSTL = 300
    kSTLstring = 365
    kStreamer = 500
    kStreamLoop = 501
    kCache = 600
    "Cache the value in memory than is not part of the object but is accessible via a SchemaRule"
    kArtificial = 1000
    kCacheNew = 1001
    kCacheDelete = 1002
    kNeedObjectForVirtualBaseClass = 99997
    kMissing = 99999
    kNoType = -1
    "Type corresponding to a 'missing' data member (with kMissing offset)"
    kUnsupportedConversion = -2
    kUnset = -3

    def __repr__(self) -> str:
        """Get a string representation of this element type."""
        return f"{self.__class__.__name__}.{self.name}"

    def is_basicpointer(self) -> bool:
        """Check if the element type is a pointer to a basic type."""
        return self.value >= self.kOffsetP and self.value < self.kOffsetP + 20

    def as_fmt(self) -> tuple[str, str]:
        """Get the format character and type name for this element type.
        Returns:
            tuple[str, str]: Type name and format character.
        Raises:
            ValueError: If the element type is not a basic type.
        """
        fmtmap = {
            self.kChar: (int, ">b"),
            self.kShort: (int, ">h"),
            self.kInt: (int, ">i"),
            self.kLong: (int, ">l"),
            self.kFloat: (float, ">f"),
            self.kDouble: (float, ">d"),
            self.kUChar: (int, ">B"),
            self.kUShort: (int, ">H"),
            self.kUInt: (int, ">I"),
            self.kULong: (int, ">Q"),
            self.kLong64: (int, ">q"),
            self.kULong64: (int, ">Q"),
            self.kBool: (bool, ">?"),
            self.kBits: (int, ">I"),
            self.kCounter: (int, ">i"),
        }
        if self not in fmtmap:
            if self < 40:
                msg = f"Reading {self!r} not implemented"
                raise NotImplementedError(msg)
            msg = f"Cannot convert {self!r} to format character"
            raise ValueError(msg)

        type_, fmt = fmtmap[self]
        return type_.__name__, fmt


@dataclass
class ArrayDim:
    dim0: int
    dim1: int
    dim2: int
    dim3: int
    dim4: int


@serializable
class TStreamerElement(TNamed):
    """TStreamerElement class.

    Reference: https://root.cern/doc/master/streamerinfo.html (TStreamerElement section)
    Also https://github.com/root-project/root/blob/b07ce7e4d93cbf50426fa881635702e48b5dc1a6/core/meta/src/TStreamerElement.cxx#L512

    Attributes:
        fType (int): Type of data described by this TStreamerElement.
        fSize (int): Size of built in type or of pointer to built in type. 0 otherwise.
        fArrayLength (int): Size of array (0 if not array)
        fArrayDim (int): Number of dimensions of array (0 if not an array)
        fMaxIndex (int[5]): Five integers giving the array dimensions (0 if not applicable)
    """

    fType: Annotated[ElementType, Fmt(">i")]
    fSize: Annotated[int, Fmt(">i")]
    fArrayLength: Annotated[int, Fmt(">i")]
    fArrayDim: Annotated[int, Fmt(">i")]
    fMaxIndex: Annotated[ArrayDim, Fmt("5i")]
    fTypeName: TString

    def member_name(self) -> str:
        """Get the member name of this streamer element."""
        return normalize(self.fName.fString)

    def cpp_typename(self) -> bytes:
        """Get the C++ type name of this streamer element."""
        return self.fTypeName.fString

    def type_name(self) -> str:
        """Get the type name of this streamer element."""
        return normalize(self.fTypeName.fString)

    def member_definition(self, parent: TStreamerInfo) -> tuple[str, list[str]]:
        """Get the member definition of this streamer element.
        Returns:
            tuple[str, list[str]]: Member definition and list of dependencies.
        """
        msg = f"member_definition not implemented for {self.__class__.__name__}"
        raise NotImplementedError(msg)


DICTIONARY["TStreamerElement"] = TStreamerElement


@serializable
class TStreamerBase(TStreamerElement):
    """Streamer element for a base class.

    Attributes:
        fBaseVersion (int): Version of base class.
    """

    fBaseVersion: Annotated[int, Fmt(">i")]


DICTIONARY["TStreamerBase"] = TStreamerBase


@serializable
class TStreamerBasicType(TStreamerElement):
    def member_definition(self, parent: TStreamerInfo):  # noqa: ARG002
        if self.fArrayLength > 0:
            msg = f"Array length {self.fArrayLength} not implemented for {self.__class__.__name__}"
            raise NotImplementedError(msg)
        type_, fmt = self.fType.as_fmt()
        return f"{self.member_name()}: Annotated[{type_}, Fmt({fmt!r})]", []


DICTIONARY["TStreamerBasicType"] = TStreamerBasicType


@serializable
class TStreamerString(TStreamerElement):
    def member_definition(self, parent: TStreamerInfo):  # noqa: ARG002
        if self.fArrayLength > 0:
            msg = f"Array length {self.fArrayLength} not implemented for {self.__class__.__name__}"
            raise NotImplementedError(msg)
        return f"{self.member_name()}: TString", []


DICTIONARY["TStreamerString"] = TStreamerString


@serializable
class TStreamerBasicPointer(TStreamerElement):
    """Streamer element for a pointer to a built in type.

    Attributes:
        fCountVersion (int): Version of count variable.
        fCountName (TString): Name of count variable.
        fCountClass (TString): Class of count variable.
    """

    fCountVersion: Annotated[int, Fmt(">i")]
    fCountName: TString
    fCountClass: TString

    def member_definition(self, parent: TStreamerInfo):
        if self.fArrayLength > 0:
            msg = f"Array length {self.fArrayLength} not implemented for {self.__class__.__name__}"
            raise NotImplementedError(msg)
        _, fmt = ElementType(self.fType - ElementType.kOffsetP).as_fmt()
        if not (
            self.fCountClass == parent.fName
            or normalize(self.fCountClass.fString) in parent.base_classes()
        ):
            msg = f"fCountClass {self.fCountClass} != parent.fName {parent.fName}"
            raise ValueError(msg)

        countname = normalize(self.fCountName.fString)
        atype = f"BasicArray({fmt!r}, {countname!r})"
        return (
            f"{self.member_name()}: Annotated[np.ndarray, {atype}]",
            [],
        )


DICTIONARY["TStreamerBasicPointer"] = TStreamerBasicPointer


@serializable
class TStreamerObject(TStreamerElement):
    def member_definition(self, parent: TStreamerInfo):
        if self.fArrayLength > 0:
            msg = f"Array length {self.fArrayLength} not implemented for {self.__class__.__name__}"
            raise NotImplementedError(msg)
        typename = self.type_name()
        dependencies = []
        if typename == parent.class_name():
            typename = f'"{typename}"'
        # elif typename == "TObjArray":
        #     typename = "TObjArray_v3"
        else:
            dependencies = [typename]
        mdef = f"{self.member_name()}: {typename}"
        return mdef, dependencies


DICTIONARY["TStreamerObject"] = TStreamerObject


@serializable
class TStreamerObjectPointer(TStreamerElement):
    def member_definition(self, parent: TStreamerInfo):
        if self.fArrayLength > 0:
            msg = f"Array length {self.fArrayLength} not implemented for {self.__class__.__name__}"
            raise NotImplementedError(msg)
        typename, dependencies = cpptype_to_pytype(self.fTypeName.fString)
        if typename == parent.class_name():
            dependencies.remove(typename)
            typename = f'"{typename}"'
        mdef = f"{self.member_name()}: Annotated[Ref[{typename}], Pointer()]"
        return mdef, list(dependencies)


DICTIONARY["TStreamerObjectPointer"] = TStreamerObjectPointer


@serializable
class TStreamerLoop(TStreamerElement):
    """Loop streamer element.

    Attributes:
        fCountVersion (int): Version of count variable.
        fCountName (TString): Name of count variable.
        fCountClass (TString): Class of count variable.
    """

    fCountVersion: Annotated[int, Fmt(">i")]
    fCountName: TString
    fCountClass: TString


DICTIONARY["TStreamerLoop"] = TStreamerLoop


@serializable
class TStreamerObjectAny(TStreamerElement):
    def member_definition(self, parent: TStreamerInfo):
        if self.fArrayLength > 0:
            msg = f"Array length {self.fArrayLength} not implemented for {self.__class__.__name__}"
            raise NotImplementedError(msg)
        if self.type_name() == parent.class_name():
            typename = f'"{self.type_name()}"'
            return f"{self.member_name()}: {typename}", []
        # This may be a non-trivial type, e.g. vector<double>
        # or vector<TLorentzVector>
        typename, dependencies = cpptype_to_pytype(self.cpp_typename())
        return f"{self.member_name()}: {typename}", list(dependencies)


DICTIONARY["TStreamerObjectAny"] = TStreamerObjectAny


@serializable
class TStreamerObjectAnyPointer(TStreamerElement):
    def member_definition(self, parent: TStreamerInfo):
        if self.fArrayLength > 0:
            msg = f"Array length {self.fArrayLength} not implemented for {self.__class__.__name__}"
            raise NotImplementedError(msg)
        typename, dependencies = cpptype_to_pytype(self.fTypeName.fString)
        if typename == parent.class_name():
            dependencies.remove(typename)
            typename = f'"{typename}"'
        mdef = f"{self.member_name()}: Annotated[Ref[{typename}], Pointer()]"
        return mdef, list(dependencies)


DICTIONARY["TStreamerObjectAnyPointer"] = TStreamerObjectAnyPointer


class STLType(IntEnum):
    kOffsetP = 40
    vector = 1
    list = 2
    deque = 3
    map = 4
    set = 5
    multimap = 6
    multiset = 7
    bitset = 8
    unordered_map = 12
    RVec = 14
    "ROOT::VecOps::RVec<T>"
    vectorPointer = kOffsetP + 1
    listPointer = kOffsetP + 2
    dequePointer = kOffsetP + 3
    mapPointer = kOffsetP + 4
    setPointer = kOffsetP + 5
    multimapPointer = kOffsetP + 6
    multisetPointer = kOffsetP + 7
    bitsetPointer = kOffsetP + 8
    string = 365


@serializable
class TStreamerSTL(TStreamerElement):
    """STL container streamer element.

    Attributes:
        fSTLtype (int): Type of STL container.
            1:vector, 2:list, 3:deque, 4:map, 5:set, 6:multimap, 7:multiset
        fCType (int): Type contained in STL container.
            Same values as for fType above, with one addition: 365:STL string

    """

    fSTLtype: Annotated[STLType, Fmt(">i")]
    fCType: Annotated[ElementType, Fmt(">i")]

    def member_definition(self, parent: TStreamerInfo):  # noqa: ARG002
        typename, dependencies = cpptype_to_pytype(self.cpp_typename())
        if self.fSTLtype == STLType.vector:
            assert typename.startswith("StdVector[")
            return f"{self.member_name()}: {typename}", list(dependencies)
        if self.fSTLtype == STLType.string:
            assert typename == "string"
            return f"{self.member_name()}: {typename}", list(dependencies)
        if self.fSTLtype == STLType.vectorPointer:
            return (
                f"{self.member_name()}: Annotated[Ref[{typename}], Pointer()]",
                list(dependencies),
            )
        msg = f"STL type {self.type_name()} not implemented yet"
        raise NotImplementedError(msg)


DICTIONARY["TStreamerSTL"] = TStreamerSTL


@serializable
class TStreamerSTLstring(TStreamerSTL):
    """STL string streamer element."""


# the lower case "string" is intentional
DICTIONARY["TStreamerSTLstring"] = TStreamerSTLstring
