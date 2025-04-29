from dataclasses import dataclass
from enum import IntEnum
from typing import Annotated, Union

from rootfilespec.bootstrap.strings import TString
from rootfilespec.bootstrap.TList import TObjArray
from rootfilespec.bootstrap.TObject import TNamed
from rootfilespec.cpptype import cpptype_to_pytype
from rootfilespec.dispatch import DICTIONARY, ENCODING, normalize
from rootfilespec.serializable import serializable
from rootfilespec.structutil import Fmt


@dataclass
class ClassDef:
    name: str
    dependencies: list[str]
    code: str


@serializable
class TStreamerInfo(TNamed):
    fCheckSum: Annotated[int, Fmt(">I")]
    fClassVersion: Annotated[int, Fmt(">i")]
    fObjects: TObjArray

    def class_name(self) -> str:
        """Get the class name of this streamer info."""
        return normalize(self.fName.fString)

    def check_classname(self) -> None:
        """We will use normalize(self.fName.fString) to lookup the class name during reading.

        But other types may be templated containers of this type so we need to ensure cpptype_to_pytype
        returns the same class name.
        """
        if (
            self.fObjects.fSize == 1
            and isinstance(self.fObjects.objects[0], TStreamerSTL)
            and self.fObjects.objects[0].fName.fString == b"This"
        ):
            # TODO: understand the purpose of these intermediate member types
            return
        if self.fName.fString.startswith(b"pair<") and self.fObjects.fSize == 2:
            return
        clsname = self.class_name()
        typename, _ = cpptype_to_pytype(self.fName.fString)
        if clsname != typename:
            msg = f"Class name mismatch: {clsname} != {typename} (raw: {self.fName.fString!r})"
            raise ValueError(msg)

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
        self.check_classname()
        bases = self.base_classes()
        # TODO: f"_VERSION = {self.fClassVersion}"
        members: list[tuple[str, str]] = []
        dependencies: list[str] = []
        for element in self.fObjects.objects:
            if not isinstance(element, (TStreamerElement, TStreamerSTLstring)):
                msg = f"Unexpected element: {element}"
                raise TypeError(msg)
            if isinstance(element, TStreamerBase):
                continue
            mdef, dep = element.member_definition(parent=self)
            dependencies.extend(dep)
            mdoc = element.fTitle.fString.decode(ENCODING).strip()
            # Prevent a syntax error from four consecutive double quotes
            if mdoc.endswith('"'):
                mdoc += " "
            members.append((mdef, mdoc))
        clsname = self.class_name()
        basestr = ", ".join(reversed(bases))
        lines: list[str] = []
        lines.append(f"# Generated for {self}")
        lines.append("@serializable")
        lines.append(f"class {clsname}({basestr}):")
        for mdef, mdoc in members:
            lines.append("    " + mdef)
            if mdoc:
                lines.append(f'    r"""{mdoc}"""')
        if not members:
            lines.append("    pass")
        lines.append("\n")
        lines.append(f"DICTIONARY['{clsname}'] = {clsname}")
        lines.append("")
        return ClassDef(clsname, bases + dependencies, "\n".join(lines))


DICTIONARY["TStreamerInfo"] = TStreamerInfo


def _structtype_to_pytype(fmt: str) -> type[Union[int, float, bool, bytes]]:
    if fmt.lstrip("<>").lower() in "bhilq":
        return int
    if fmt.lstrip("<>").lower() in "fd":
        return float
    if fmt.lstrip("<>") == "?":
        return bool
    if fmt in ("float16", "double32"):
        return float
    if fmt == "charstar":
        return bytes
    msg = f"Unknown format character {fmt!r}"
    raise ValueError(msg)


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

    def is_array(self) -> bool:
        """Check if the element type is an array."""
        return self.value >= self.kOffsetL and self.value < self.kOffsetL + 20

    def is_basicpointer(self) -> bool:
        """Check if the element type is a pointer to a basic type."""
        return self.value >= self.kOffsetP and self.value < self.kOffsetP + 20

    def as_fmt(self) -> str:
        """Get the format character for this element type."""
        fmtmap = {
            self.kChar: ">b",
            self.kShort: ">h",
            self.kInt: ">i",
            self.kLong: ">l",
            self.kFloat: ">f",
            self.kDouble: ">d",
            self.kUChar: ">B",
            self.kUShort: ">H",
            self.kUInt: ">I",
            self.kULong: ">Q",
            self.kLong64: ">q",
            self.kULong64: ">Q",
            self.kBool: ">?",
            self.kBits: ">I",
            self.kCounter: ">i",
            self.kCharStar: "charstar",
            self.kDouble32: "double32",
            self.kFloat16: "float16",
        }
        if self not in fmtmap:
            msg = f"Cannot convert {self!r} to format character"
            raise ValueError(msg)

        return fmtmap[self]


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
            fmt = ElementType(self.fType - ElementType.kOffsetL).as_fmt()
            atype = f"FixedSizeArray({fmt!r}, {self.fArrayLength})"
            return f"{self.member_name()}: Annotated[np.ndarray, {atype}]", []
        fmt = self.fType.as_fmt()
        pytype = _structtype_to_pytype(fmt).__name__
        return f"{self.member_name()}: Annotated[{pytype}, Fmt({fmt!r})]", []


DICTIONARY["TStreamerBasicType"] = TStreamerBasicType


@serializable
class TStreamerString(TStreamerElement):
    def member_definition(self, parent: TStreamerInfo):  # noqa: ARG002
        if self.fArrayLength > 0:
            msg = f"Array length not implemented for {self.__class__.__name__}"
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
            msg = f"Array length not implemented for {self.__class__.__name__}"
            raise NotImplementedError(msg)

        fmt = ElementType(self.fType - ElementType.kOffsetP).as_fmt()
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
            msg = f"Array length not implemented for {self.__class__.__name__}"
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
            msg = f"Array length not implemented for {self.__class__.__name__}"
            raise NotImplementedError(msg)
        assert self.fTypeName.fString.endswith(b"*")
        typename, dependencies = cpptype_to_pytype(self.fTypeName.fString)
        this = parent.class_name()
        if this in dependencies:
            dependencies.remove(this)
            typename = typename.replace(this, f'"{this}"')
        mdef = f"{self.member_name()}: {typename}"
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
            msg = f"Array length not implemented for {self.__class__.__name__}"
            raise NotImplementedError(msg)
        if self.type_name() == parent.class_name():
            typename = f'"{self.type_name()}"'
            return f"{self.member_name()}: {typename}", []
        # This may be a non-trivial type, e.g. vector<double>
        # or vector<TLorentzVector>
        typename, dependencies = cpptype_to_pytype(self.fTypeName.fString)
        return f"{self.member_name()}: {typename}", list(dependencies)


DICTIONARY["TStreamerObjectAny"] = TStreamerObjectAny


@serializable
class TStreamerObjectAnyPointer(TStreamerElement):
    def member_definition(self, parent: TStreamerInfo):
        if self.fArrayLength > 0:
            msg = f"Array length not implemented for {self.__class__.__name__}"
            raise NotImplementedError(msg)
        assert self.fTypeName.fString.endswith(b"*")
        typename, dependencies = cpptype_to_pytype(
            self.fTypeName.fString.removesuffix(b"*")
        )
        if typename == parent.class_name():
            dependencies.remove(typename)
            typename = f'"{typename}"'
        mdef = f"{self.member_name()}: Ref[{typename}]"
        return mdef, list(dependencies)


DICTIONARY["TStreamerObjectAnyPointer"] = TStreamerObjectAnyPointer


class STLType(IntEnum):
    """STL container type codes.

    https://github.com/root-project/root/blob/v6-34-08/core/foundation/inc/ESTLType.h#L28
    """

    kOffsetP = 40
    vector = 1
    list = 2
    deque = 3
    map = 4
    multimap = 5
    set = 6
    multiset = 7
    bitset = 8
    forwardlist = 9
    unorderedset = 10
    unorderedmultiset = 11
    unorderedmap = 12
    unorderedmultimap = 13
    RVec = 14
    "ROOT::VecOps::RVec<T>"
    kSTLend = 15
    vectorPointer = kOffsetP + vector
    listPointer = kOffsetP + list
    dequePointer = kOffsetP + deque
    mapPointer = kOffsetP + map
    multimapPointer = kOffsetP + multimap
    setPointer = kOffsetP + set
    multisetPointer = kOffsetP + multiset
    bitsetPointer = kOffsetP + bitset
    forwardlistPointer = kOffsetP + forwardlist
    unorderedsetPointer = kOffsetP + unorderedset
    unorderedmultisetPointer = kOffsetP + unorderedmultiset
    unorderedmapPointer = kOffsetP + unorderedmap
    unorderedmultimapPointer = kOffsetP + unorderedmultimap
    string = 365

    def __repr__(self) -> str:
        """Get a string representation of this element type."""
        return f"{self.__class__.__name__}.{self.name}"


_cpp_primitives = {
    "bool": "Annotated[bool, Fmt('>?')]",
    "char": "Annotated[int, Fmt('>b')]",
    "unsigned char": "Annotated[int, Fmt('>B')]",
    "short": "Annotated[int, Fmt('>h')]",
    "unsigned short": "Annotated[int, Fmt('>H')]",
    "int": "Annotated[int, Fmt('>i')]",
    "unsigned int": "Annotated[int, Fmt('>I')]",
    "Long64_t": "Annotated[int, Fmt('>q')]",
    "long": "Annotated[int, Fmt('>q')]",
    "unsigned long": "Annotated[int, Fmt('>Q')]",
    "float": "Annotated[float, Fmt('>f')]",
    "double": "Annotated[float, Fmt('>d')]",
}


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
        if STLType.kOffsetP <= self.fSTLtype < STLType.kOffsetP + STLType.kSTLend:
            assert self.fTypeName.fString.endswith(b"*")
        typename, dependencies = cpptype_to_pytype(self.fTypeName.fString)
        return f"{self.member_name()}: {typename}", list(dependencies)


DICTIONARY["TStreamerSTL"] = TStreamerSTL


@serializable
class TStreamerSTLstring(TStreamerSTL):
    """STL string streamer element."""

    def member_definition(self, parent: TStreamerInfo):  # noqa: ARG002
        return f"{self.member_name()}: STLString", []


# the lower case "string" is intentional
DICTIONARY["TStreamerSTLstring"] = TStreamerSTLstring
