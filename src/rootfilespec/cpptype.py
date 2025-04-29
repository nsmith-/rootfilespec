"""A parser for C++ types to convert them to Python types.

This is used in bootstrap/TStreamerInfo.py to parse the C++ types in the
TStreamerInfo class. The grammar is far from the full C++ EBNF grammar,
but it is good enough for the types we need to parse.

A potential improvement would be to use a lark standalone parser, if this
one proves too simple. The grammar this implements should be equivalent to:

```lark
%import common.WS_INLINE
%ignore WS_INLINE
%import common.CNAME

start: value
?value: ["const"] (template | typeid) "*"?
template: template_name "<" template_args ">"
template_name: CNAME
template_args: value ("," value)*
typeid: CNAME (CNAME)*
```
which can be rendered with lark using `python -m lark.tools.standalone cpptype.lark`
"""

import re
from dataclasses import dataclass
from enum import IntEnum
from typing import Optional, Protocol

from rootfilespec.dispatch import normalize

_cpp_primitives = {
    b"bool": "Annotated[bool, Fmt('>?')]",
    b"char": "Annotated[int, Fmt('>b')]",
    b"unsigned char": "Annotated[int, Fmt('>B')]",
    b"short": "Annotated[int, Fmt('>h')]",
    b"unsigned short": "Annotated[int, Fmt('>H')]",
    b"int": "Annotated[int, Fmt('>i')]",
    b"unsigned int": "Annotated[int, Fmt('>I')]",
    b"Long64_t": "Annotated[int, Fmt('>q')]",
    b"long": "Annotated[int, Fmt('>q')]",
    b"unsigned long": "Annotated[int, Fmt('>Q')]",
    b"ULong64_t": "Annotated[int, Fmt('>Q')]",
    b"float": "Annotated[float, Fmt('>f')]",
    b"double": "Annotated[float, Fmt('>d')]",
}
# cppname -> python name, expected number of template arguments
_cpp_templates: dict[bytes, tuple[str, int]] = {
    b"vector": ("StdVector", 1),
    b"set": ("StdSet", 1),
    b"deque": ("StdDeque", 1),
    b"map": ("StdMap", 2),
    b"pair": ("StdPair", 2),
}


_tokenize = re.compile(rb"(const |[\w:]+|<| ?>|, ?| ?\*| )")


class _TokenType(IntEnum):
    NAME = 0
    TEMPLATE_START = 1
    TEMPLATE_END = 2
    COMMA = 3
    POINTER = 4
    SPACE = 5
    CONSTQUAL = 6
    """Const qualifier should be ignorable in serialization"""


class _Token:
    type: _TokenType
    value: bytes

    def __init__(self, match: bytes):
        self.value = match
        if match == b"<":
            self.type = _TokenType.TEMPLATE_START
        elif match.endswith(b">"):
            self.type = _TokenType.TEMPLATE_END
        elif match.startswith(b","):
            self.type = _TokenType.COMMA
        elif match.endswith(b"*"):
            self.type = _TokenType.POINTER
        elif match == b" ":
            self.type = _TokenType.SPACE
        elif match == b"const ":
            self.type = _TokenType.CONSTQUAL
        else:
            self.type = _TokenType.NAME

    def __repr__(self) -> str:
        return f"_Token(type={self.type.name}, value={self.value!r})"


class _TokenStream:
    def __init__(self, input: bytes):
        split = _tokenize.split(input)
        if any(split[::2]):
            msg = f"Failed to split: unexpected token(s) in C++ type name {input!r}"
            raise ValueError(msg)
        self._tokens = iter(_Token(match) for match in split[1::2])
        self._current = next(self._tokens, None)

    def peek(self) -> Optional[_Token]:
        return self._current

    def next(self) -> Optional[_Token]:
        token, self._current = self._current, next(self._tokens, None)
        return token


NameWithDependencies = tuple[str, set[str]]


class _CppTypeAstNode(Protocol):
    def cppname(self) -> bytes:
        """Return the C++ name of the type."""
        ...

    def to_pytype(self) -> NameWithDependencies:
        """Convert C++ type name to Python type name."""
        ...


@dataclass
class _CppTypeAstName(_CppTypeAstNode):
    name: bytes

    def cppname(self) -> bytes:
        return self.name

    def to_pytype(self) -> NameWithDependencies:
        """Convert C++ type name to Python type name."""
        if self.name in _cpp_primitives:
            deps: set[str] = set()
            return _cpp_primitives[self.name], deps
        pyname = normalize(self.name)
        return pyname, {pyname}


@dataclass
class _CppTypeAstTemplate(_CppTypeAstName):
    args: tuple[_CppTypeAstNode, ...]

    def cppname(self) -> bytes:
        args = b",".join(arg.cppname() for arg in self.args)
        close = b">"
        if isinstance(self.args[-1], _CppTypeAstTemplate):
            close = b" >"
        return self.name + b"<" + args + close

    def to_pytype(self) -> NameWithDependencies:
        """Convert C++ type name to Python type name."""
        deps: set[str] = set()
        if self.name == b"bitset":
            argn, *rest = self.args
            if rest:
                msg = "bitset template has too many arguments"
                raise ValueError(msg)
            if not isinstance(argn, _CppTypeAstName):
                msg = "bitset template argument must be a name"
                raise ValueError(msg)
            n = int(argn.name)
            pyname = f"Annotated[int, StdBitset({n})]"
            return pyname, deps
        if self.name not in _cpp_templates:
            # This is probably a templated data member and not a container type
            # So it should be in the dictionary and we just have to normalize the name
            # TODO: is there any way to tell when we have a container or not?
            pyname = normalize(self.cppname())
            return pyname, {pyname}
        # This is a C++ template type we know how to handle
        pyname, nargs = _cpp_templates[self.name]
        args = []
        if len(self.args) > nargs:
            # TODO: warn when we have extra arguments?
            # these can be e.g. the comparison argument in std::map (defaults to std::less<Key>)
            pass
        for arg in self.args[:nargs]:
            argname, argdeps = arg.to_pytype()
            args.append(argname)
            deps = deps.union(argdeps)
        return f"{pyname}[{', '.join(args)}]", deps


@dataclass
class _CppTypeAstPointer(_CppTypeAstNode):
    arg: _CppTypeAstName

    def cppname(self) -> bytes:
        return self.arg.cppname() + b"*"

    def to_pytype(self) -> NameWithDependencies:
        arg, deps = self.arg.to_pytype()
        return f"Ref[{arg}]", deps


def _template_args(stream: _TokenStream) -> tuple[_CppTypeAstNode, ...]:
    token = stream.next()
    args: tuple[_CppTypeAstNode, ...] = ()
    while True:
        token = stream.peek()
        if not token:
            msg = "Unexpected end of stream"
            raise ValueError(msg)
        if token.type == _TokenType.TEMPLATE_END:
            stream.next()
            break
        if token.type == _TokenType.COMMA:
            stream.next()
            continue
        arg = _value(stream)
        args = (*args, arg)
    return args


def _value(stream: _TokenStream) -> _CppTypeAstNode:
    token = stream.next()
    if not token:
        msg = "Unexpected end of stream"
        raise ValueError(msg)
    if token.type == _TokenType.CONSTQUAL:
        # Ignore const qualifier
        token = stream.next()
        if not token:
            msg = "Unexpected end of stream after const"
            raise ValueError(msg)
    if token.type != _TokenType.NAME:
        msg = f"Unexpected token {token}"
        raise ValueError(msg)
    name = token.value
    token = stream.peek()
    if not token or token.type in (
        _TokenType.TEMPLATE_END,
        _TokenType.COMMA,
        _TokenType.POINTER,
    ):
        # we are in a simple type
        arg = _CppTypeAstName(name)
        if token and token.type == _TokenType.POINTER:
            # wrap arg in a pointer type
            stream.next()
            return _CppTypeAstPointer(arg)
        return arg
    if token.type == _TokenType.TEMPLATE_START:
        # we are in template rule
        return _CppTypeAstTemplate(name, _template_args(stream))
    if token.type in (_TokenType.NAME, _TokenType.SPACE):
        # we are in typeid
        while (token := stream.peek()) and token.type in (
            _TokenType.NAME,
            _TokenType.SPACE,
        ):
            name += token.value
            stream.next()
        return _CppTypeAstName(name)
    msg = f"Unexpected token {token}"
    raise ValueError(msg)


def parse_cpptype(cppname: bytes) -> _CppTypeAstNode:
    stream = _TokenStream(cppname)
    return _value(stream)


def normalize_cpptype(cppname: bytes) -> bytes:
    """Normalize C++ type name
    Any spaces in templates will be elided.
    """
    ast = parse_cpptype(cppname)
    return ast.cppname()


def cpptype_to_pytype(cppname: bytes) -> NameWithDependencies:
    """Convert C++ type name to Python type name.

    This uses a very simple parser that only handles the types we need.
    """
    ast = parse_cpptype(cppname)
    return ast.to_pytype()
