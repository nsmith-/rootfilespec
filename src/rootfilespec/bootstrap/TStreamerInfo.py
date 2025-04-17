from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import Annotated

from rootfilespec.bootstrap.TList import TObjArray
from rootfilespec.bootstrap.TObject import TNamed
from rootfilespec.bootstrap.TString import TString
from rootfilespec.dispatch import DICTIONARY, normalize
from rootfilespec.structutil import Fmt, ReadBuffer, serializable


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

    def class_definition(self) -> ClassDef:
        """Get the class definition code of this streamer info."""
        bases: list[str] = []
        members: list[str] = []
        dependencies: list[str] = []
        for element in self.fObjects.objects:
            assert isinstance(element, TStreamerElement)
            if isinstance(element, TStreamerBase):
                bases.append(element.member_name())
                continue
            mdef, dep = element.member_definition(parent=self)
            dependencies.extend(dep)
            members.append(mdef)
        clsname = self.class_name()
        if len(bases) == 0:
            bases.append("StreamedObject")
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

    Built in types:
    1:char, 2:short, 3:int, 4:long, 5:float, 8:double
    11, 12, 13, 14:unsigned char, short, int, long respectively
    6: an array dimension (counter)
    15: bit mask (used for fBits field)

    Pointers to built in types:
    40 + fType of built in type (e.g. 43: pointer to int)

    Objects:
    65:TString, 66:TObject, 67:TNamed
    0: base class (other than TObject or TNamed)
    61: object data member derived from TObject (other than TObject or TNamed)
    62: object data member not derived from TObject
    63: pointer to object derived from TObject (pointer can't be null)
    64: pointer to object derived from TObject (pointer may be null)
    501: pointer to an array of objects
    500: an STL string or container

    Arrays:
    20 + fType of array element (e.g. 23: array of int)
    """

    kChar = 1
    kShort = 2
    kInt = 3
    kLong = 4
    kFloat = 5
    kDouble = 8
    kUChar = 11
    kUShort = 12
    kUInt = 13
    kULong = 14
    kLong64 = 16
    kBool = 18
    kArrayDim = 6
    kBitMask = 15
    kPointer = 40
    kPointerChar = 41
    kPointerShort = 42
    kPointerInt = 43
    kPointerLong = 44
    kPointerFloat = 45
    kPointerDouble = 48
    kPointerUChar = 51
    kPointerUShort = 52
    kPointerUInt = 53
    kPointerULong = 54
    kPointerLong64 = 56
    kTString = 65
    kTObject = 66
    kTNamed = 67
    kBaseClass = 0
    kObjectDataMemberTObject = 61
    kObjectDataMember = 62
    kPointerTObjectNotNull = 63
    kPointerTObjectNullable = 64
    kPointerArray = 501
    kSTL = 500

    def __repr__(self) -> str:
        """Get a string representation of this element type."""
        return f"{self.__class__.__name__}.{self.name}"

    def is_basicpointer(self) -> bool:
        """Check if the element type is a pointer to a basic type."""
        return self.value >= self.kPointer and self.value < self.kPointer + 20

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
            self.kBool: (bool, ">?"),
            self.kBitMask: (int, ">I"),
            self.kArrayDim: (int, ">i"),
        }
        if self not in fmtmap:
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

    def type_name(self) -> str:
        """Get the type name of this streamer element."""
        return normalize(self.fTypeName.fString)

    def member_definition(self, parent: TStreamerInfo) -> tuple[str, list[str]]:
        """Get the member definition of this streamer element.
        Returns:
            tuple[str, list[str]]: Member definition and list of dependencies.
        """
        raise NotImplementedError


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
        type_, fmt = self.fType.as_fmt()
        return f"{self.member_name()}: Annotated[{type_}, Fmt({fmt!r})]", []


DICTIONARY["TStreamerBasicType"] = TStreamerBasicType


@serializable
class TStreamerString(TStreamerElement):
    def member_definition(self, parent: TStreamerInfo):  # noqa: ARG002
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
        _, fmt = ElementType(self.fType - ElementType.kPointer).as_fmt()
        if self.fCountClass != parent.fName:
            msg = f"fCountClass {self.fCountClass} != parent.fName {parent.fName}"
            raise ValueError(msg)

        countname = normalize(self.fCountName.fString)
        atype = f"BasicArray(np.dtype({fmt!r}), {countname!r})"
        return (
            f"{self.member_name()}: Annotated[np.ndarray, {atype}]",
            [],
        )


DICTIONARY["TStreamerBasicPointer"] = TStreamerBasicPointer


@serializable
class TStreamerObject(TStreamerElement):
    def member_definition(self, parent: TStreamerInfo):
        typename = self.type_name()
        dependencies = []
        if typename == parent.class_name():
            typename = "Self"
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
        typename = self.type_name()
        dependencies = []
        assert typename.endswith("*")
        typename = typename[:-1]
        if typename == parent.class_name():
            typename = "Self"
        else:
            dependencies = [typename]
        if self.fType == ElementType.kPointerTObjectNullable:
            typename = f"Pointer[{typename}]"
        mdef = f"{self.member_name()}: {typename}"
        return mdef, dependencies


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
    def member_definition(self, parent: TStreamerInfo):  # noqa: ARG002
        return f"{self.member_name()}: {self.type_name()}", []


DICTIONARY["TStreamerObjectAny"] = TStreamerObjectAny


@serializable
class TStreamerSTL(TStreamerElement):
    """STL container streamer element.

    Attributes:
        fSTLtype (int): Type of STL container.
            1:vector, 2:list, 3:deque, 4:map, 5:set, 6:multimap, 7:multiset
        fCType (int): Type contained in STL container.
            Same values as for fType above, with one addition: 365:STL string

    """

    fSTLtype: Annotated[int, Fmt(">i")]
    fCType: Annotated[int, Fmt(">i")]

    def member_definition(self, parent: TStreamerInfo):  # noqa: ARG002
        if self.type_name() == "vector<string>":
            typename = "StdVector[TString]"
            return f"{self.member_name()}: {typename}", []
        msg = f"STL type {self.type_name()} not implemented yet"
        raise NotImplementedError(msg)


DICTIONARY["TStreamerSTL"] = TStreamerSTL


@serializable
class TStreamerSTLstring(TStreamerElement):
    """STL string streamer element.

    Attributes:
        uninterpreted (bytes): Uninterpreted data.
            TODO: According to ROOT docs, there should not be any extra data here
    """

    uninterpreted: bytes

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        msg = "TStreamerSTLString.read_members"
        raise NotImplementedError(msg)


# the lower case "string" is intentional
DICTIONARY["TStreamerSTLstring"] = TStreamerSTLstring
