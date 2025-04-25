# Design of this package

This package is designed to deserialize binary data stored in ROOT files. It is
expected to be used in the context of another package, such as uproot, to fetch
data buffers (either locally or over the network) and provide them to this
package. It is also out of scope to take deserialized data and interpret it as
numpy or awkward arrays, with some minor exceptions when performance is
critical.

All deserialized data is stored as dataclass objects, inheriting from the base
`ROOTSerializable` type. This type has two main methods:

```python
Members = dict[str, Any]


@dataclasses.dataclass
class ROOTSerializable:
    @classmethod
    def read(cls: type[T], buffer: ReadBuffer) -> tuple[T, ReadBuffer]: ...

    @classmethod
    def update_members(
        cls, members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]: ...
```

The entry point for deserialization is the `read` method, which calls
`update_members` on all the subclasses in the inheritance tree to build up the
dictionary of class members (`Members`). Some classes may override `read` to
implement header parsing, or to handle layouts that are not simply in base class
order. The `read_mupdate_membersembers` method is only responsible for reading
the members of the class, not any base class members.

The `update_members` signature has a type alias in `serializable.py`:

```python
ReadMembersMethod = Callable[[Members, ReadBuffer], tuple[Members, ReadBuffer]]
```

these is used to define more advanced types, and to set up the `@serializable`
decorator.

Note that python dataclasses inherit members from multiple bases in reverse
order, so

```python
@dataclass
class Base1:
    a: int
    b: int


@dataclass
class Base2:
    c: int
    d: int


@dataclass
class Derived(Base1, Base2):
    e: int
```

will have the following order of members: `c, d, a, b, e`.

## Deserialization of numeric types

We use python `struct` to read numeric types from the buffer. Classes that
contain numeric members can be declared as:

```python
@serializable
class TMyClass(ROOTSerializable):
    fInt: Annotated[int, Fmt(">i")]
    fFloat: Annotated[float, Fmt(">f")]
```

where the `Fmt` type is a descriptor that will be used by the `@serializable`
decorator to auto-generate the appropriate `read_members` implementation.

## Annotated builtin types vs. objects

In several places, we have the option to "pythonize" the data structure, by
using builtin python types where they fully capture the semantics of a given
ROOT type. For example, `TString` is a variable length bytestring, and can be
represented as a `bytes` in python. We could have class members, such as the
name and title of a `TNamed`, either be represented by `TString`:

```python
@serializable
class TString(ROOTSerializable):
    fString: bytes


@serializable
class TNamed(TObject):
    fName: TString
    fTitle: TString
```

or by `bytes`:

```python
@serializable
class TNamed(TObject):
    fName: Annotated[bytes, TString]
    fTitle: Annotated[bytes, TString]
```

where the `@serializable` decorator takes care of the conversion between `str`
and `TString`. In this library, we will prefer to use the second approach when
feasible.
