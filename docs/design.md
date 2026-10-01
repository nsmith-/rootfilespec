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
order. The `update_members` method is only responsible for reading the members
of the class, not any base class members.

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
    fName: Annotated[bytes, ROOTString("TString")]
    fTitle: Annotated[bytes, ROOTString("TString")]
```

where the `ROOTString` annotation tells the `@serializable` decorator how the
bytes are encoded on disk. In this library, we will prefer to use the second
approach when feasible. Every string type is read this way: `ROOTString` has one
encoding per length format (`"TString"`, `"charstar"` and `"RNTuple"`), and
`framed=True` when a byte count and version word come first, as for a
`std::string` data member. `bootstrap.TString` is the alias
`Annotated[bytes, ROOTString("TString")]` for hand-written classes.

A builtin does not record which ROOT type or encoding it came from, so the rule
is: **a value may be a builtin only inside a context that records its ROOT type
and encoding**. Writing it back needs that context, not just the value. The
contexts are:

- a data member: its annotation;
- a container element (e.g. `vector<string>`): the container's element type;
- data written with a `TKey`: the key's `fClassName`. The caller keeps the key
  and the value it located as a pair, so a key of class `TString` reads as
  `bytes`;
- an object reached through a pointer: its `Ref`, which keeps the stream header
  (the class tag or back-reference, the byte count, the version). Every pointee
  is wrapped, so a `TList` reads as a list of `Ref`s. This is #105, still open:
  today a pointee with a new class tag comes back bare and its header is
  dropped, so a string read through a pointer cannot be written back as read.

#139 surveys every way an object reaches a file, including those not read yet
(unsplit `TTree` branches, RNTuple streamer fields, `TClonesArray` elements). A
reader for one of them keeps the record of the type next to the values.

Which builtin to use is a matter of ergonomics: `bytes` for strings, never
decoded; and for example a `datetime` rather than the packed `int` for a
`TDatime` (#123).

The conversion from the bytes on disk to the builtin must be injective on every
input the reader accepts: inputs that would decode to the same value are
rejected with a clear error, or kept, never merged, because a writer could not
tell which one to put back. For example, a `kBool` byte can be `0x99` in files
ROOT writes (root-io-spec ElementTypes §2.5), so reading it as `bool` loses the
byte; and a `TString` can be read from the long form even when it is short
(#140).

## Keeping what is on disk

Writing files is a goal, so a deserialized object keeps everything a writer
would need to write the same bytes back: stored values as stored (a sign that
carries a flag, a checksum, unknown trailing bytes), with convenience properties
derived from them rather than in their place.

## Content the parser does not understand

When a file holds something the parser does not understand (an unknown feature
flag, an unknown type), it raises a clear error or keeps the bytes
uninterpreted, rather than guessing. A class missing from the StreamerInfo is to
read as `Uninterpreted`, skipped by its byte count (#74, still open: today it
raises `Unknown type ...`).

## Generated classes

`dynamic.py` generates dataclasses from a file's `TStreamerInfo`. The agreed
direction (#67) is for the streamer info to steer deserialization at runtime,
with the generated dataclasses kept only as the result, so new work should not
build more on annotations and inheritance as the serialization definition.
Generated model names are to carry the StreamerInfo checksum, with the class
version in the docstring (#22, still open: today a generated class has the bare
class name).

## Data Fetching and Locators

Many ROOT objects serve as references to data stored elsewhere in the file. For
example, a `TKey` points to a serialized object, an `REnvelopeLink` points to an
RNTuple envelope, and `TDirectory` points to a key list. To enable flexible I/O
patterns (synchronous, asynchronous, or batch fetching), this library separates
**data location** from **data fetching**.

### The Locator Pattern

The design follows this principle: **objects describe where data is; callers
decide when and how to fetch it**.

Each object that points to data provides a `*_locator` property that returns a
specialized locator object. The locator has:

- `offset: int` - byte offset in the file
- `size: int` - size of the data to be read
- `read_from(buffer) -> T` - deserializes the specific data type from the buffer

### Specialized Locator Classes

Even though `read_from` might appear to be implementable as `T.read(buffer)`,
sometimes a locator points to some wrapper of the data. For example, a `TKey`
points to the data prefixed by essentially another copy of itself, as a
validation check. This method allows the locator to handle the unwrapping logic,
and to return the correct type.

### Usage Pattern

```python
# 1. Get the locator (no I/O)
loc = root_file.tfile_locator
print(f"TFile at offset {loc.offset}, size {loc.size}")

# 2. Fetch the data (caller controls I/O)
buffer = fetch_data(loc.offset, loc.size)

# 3. Deserialize (locator knows the type)
tfile = loc.read_from(buffer)
```

### Batch Fetching Example

```python
# Collect all locators first (no I/O)
tfile_loc = root_file.tfile_locator
si_loc = root_file.streamerinfo_locator
locators = [tfile_loc, si_loc] if si_loc else [tfile_loc]

# Batch fetch (optimization opportunity)
# - Sort by offset for sequential reads
# - Combine nearby reads
# - Parallelize independent fetches
# - Check cache before fetching
buffers = [fetch_data(loc.offset, loc.size) for loc in locators]

# Deserialize all at once
tfile = tfile_loc.read_from(buffers[0])
streamerinfo = si_loc.read_from(buffers[1]) if si_loc else None
```

### A minimal synchronous reader

For the common case of one blocking read per locator, `rootfilespec.reader`
wraps the pattern above. It still does no I/O of its own: `FileReader.open`
takes a function returning the bytes at `(offset, size)`, and `open_path` is
that function for a local file.

```python
from rootfilespec.reader import open_path

with open_path("file.root") as reader:
    # reader.file, reader.tfile and reader.streamerinfo are already read, and
    # reader.fetch interprets data with the classes of the StreamerInfo record
    keylist = reader.keylist()  # of reader.rootdir, or of any TDirectory
    obj = reader.fetch(keylist[b"name"])  # a TKey is a locator
    element = reader.streamerinfos()[b"TNamed"].element(b"fName")
```

`reader.fetch(loc)` is `loc.read_from(buffer)` on the fetched bytes;
`reader.fetch.resolve(loc)` is for the locators that return the key at the front
of a record (`tfile_locator`, `streamerinfo_locator`, `keylist_locator`) and
reads the record as well; `reader.fetch.buffer(loc)` only fetches, which is the
callable `RNTuple.from_anchor` expects.
