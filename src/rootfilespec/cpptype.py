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
?value: ["const"] (template | typeid) ["*"]
template: template_name "<" template_args ">"
template_name: CNAME
template_args: value ("," value)*
typeid: CNAME (CNAME)*
```
which can be rendered with lark using `python -m lark.tools.standalone cpptype.lark`
"""

import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Optional

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
    b"float": "Annotated[float, Fmt('>f')]",
    b"double": "Annotated[float, Fmt('>d')]",
}
_cpp_templates = {
    b"vector": "StdVector",
}


_tokenize = re.compile(rb"([\w:]+)|([<>,\*])|(?:\s+)")
_Token = tuple[bytes, bytes]
_ignored_terminals: set[_Token] = {
    (b"", b""),
    (b"const", b""),
    (b"", b"*"),
    # Workaround for Pypy bug https://github.com/pypy/pypy/issues/5265
    ("", ""),  # type: ignore[arg-type]
    (b"const", ""),  # type: ignore[arg-type]
    ("", b"*"),  # type: ignore[arg-type]
}


class _TokenStream:
    def __init__(self, tokens: Iterable[_Token]):
        self._tokens = iter(tokens)
        self._current = next(self._tokens, None)

    def peek(self) -> Optional[_Token]:
        return self._current

    def next(self) -> Optional[_Token]:
        token, self._current = self._current, next(self._tokens, None)
        return token


@dataclass
class _CppTypeAstNode:
    name: bytes

    def to_pytype(self) -> tuple[str, set[str]]:
        """Convert C++ type name to Python type name."""
        if self.name in _cpp_primitives:
            return _cpp_primitives[self.name], set()
        pyname = normalize(self.name)
        return pyname, {pyname}


@dataclass
class _CppTypeAstTemplate(_CppTypeAstNode):
    args: tuple[_CppTypeAstNode, ...]

    def to_pytype(self):
        """Convert C++ type name to Python type name."""
        if self.name in _cpp_templates:
            pyname = _cpp_templates[self.name]
        else:
            msg = f"Template type {self.name!r} not implemented"
            raise NotImplementedError(msg)
        args = []
        deps: set[str] = set()
        for arg in self.args:
            argname, argdeps = arg.to_pytype()
            args.append(argname)
            deps = deps.union(argdeps)
        return f"{pyname}[{', '.join(args)}]", deps


def _template_args(stream: _TokenStream) -> tuple[_CppTypeAstNode, ...]:
    token = stream.next()
    args: tuple[_CppTypeAstNode, ...] = ()
    while True:
        token = stream.peek()
        if not token:
            msg = "Unexpected end of stream"
            raise ValueError(msg)
        if token[1] == b">":
            stream.next()
            break
        if token[1] == b",":
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
    if token[1]:
        msg = f"Unexpected token {token}"
        raise ValueError(msg)
    name = token[0]
    token = stream.peek()
    if not token or token[1] in (b">", b","):
        # we are in a simple type
        return _CppTypeAstNode(name)
    if token[1] == b"<":
        # we are in template rule
        return _CppTypeAstTemplate(name, _template_args(stream))
    if not token[1]:
        # we are in typeid
        while (token := stream.peek()) and not token[1]:
            name += b" " + token[0]
            stream.next()
        return _CppTypeAstNode(name)
    msg = f"Unexpected token {token}"
    raise ValueError(msg)


def cpptype_to_pytype(cppname: bytes) -> tuple[str, set[str]]:
    """Convert C++ type name to Python type name.

    This uses a very simple parser that only handles the types we need.
    """
    alltokens: list[_Token] = _tokenize.findall(cppname)
    stream = _TokenStream(t for t in alltokens if t not in _ignored_terminals)
    ast = _value(stream)
    return ast.to_pytype()
