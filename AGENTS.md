# AGENTS.md

Guidance for coding agents (and people) working on rootfilespec. Read
`README.md` and `docs/design.md` first.

## What this package is

rootfilespec parses ROOT file binary data into Python dataclasses of primitive
types and numpy arrays. It does no I/O of its own: it takes bytes buffers and
returns objects. The goal is a stable, complete read (and later write) backend
for packages such as uproot.

- `src/rootfilespec/bootstrap/`: the self-describing part of a file (`TFile`,
  `TKey`, `TDirectory`, `TStreamerInfo`, strings, compression) and the RNTuple
  anchor.
- `src/rootfilespec/rntuple/`: RNTuple envelopes, schema and page locations.
- `src/rootfilespec/dynamic.py`: generates classes from a file's
  `TStreamerInfo`. The agreed direction (#67) is for the streamer info to steer
  deserialization at runtime, with the generated dataclasses kept only as the
  result; don't build more on annotations and inheritance as the serialization
  definition.
- `src/rootfilespec/reader.py`: `FileReader` and `Fetcher`, the I/O side.

## Design rules

- **Objects describe where data is; callers fetch.** A locator has an `offset`,
  a `size` and `read_from(buffer)`. Parsing code never reads from a file itself.
- **Builtin types where they capture the ROOT type, as long as the ROOT type
  stays recoverable.** Every string (key names included) is `bytes`, never
  decoded. Its encoding lives in the annotation
  (`Annotated[bytes, ROOTString(...)]`) or in the enclosing record (a key's
  `fClassName`), not in a wrapper class. Where neither holds it, keep something
  that does, or document that the value cannot be written back as read.
- **Keep what is on disk.** Keep everything a writer would need to write the
  bytes back: stored values as stored (a sign that carries a flag, a checksum,
  unknown trailing bytes), with convenience properties derived from them.
- **Don't guess.** When a file has something the parser doesn't understand (an
  unknown feature flag, an unknown type), raise a clear error or keep the bytes
  uninterpreted, rather than misread it. A class missing from the StreamerInfo
  reads as `Uninterpreted`, skipped by its byte count (#74).
- Generated model names carry the StreamerInfo checksum; the class version goes
  in the docstring.

## Format references

- The reference for every format question is root-io-spec, vendored as a
  submodule at `reference/root-io-spec`. Cite its sections (and the ROOT source
  lines it cites) in code comments, commit messages and PR descriptions.
- For RNTuple, read `spec/05-rntuple/BinaryFormatSpecification.md` together with
  `ERRATA.md` and `NOTES.md` in the same directory: ROOT's document disagrees
  with ROOT's code in places, and those files record where.
- Do not infer the format from this parser. It can be wrong, and the fixes go
  here.
- `reference/root-io-spec/data/` holds small, byte-documented fixtures. Each
  case's `gen/cases/<case>/case.toml` pins values at file offsets.

## Setup

```sh
git submodule update --init reference/root-io-spec   # never --recursive: it nests all of ROOT (~1.5 GB)
uv sync --group dev                                  # as CI does, from uv.lock
uv tool install pre-commit && pre-commit install     # pre-commit is not in the dev group
```

Without uv: `python -m venv .venv && source .venv/bin/activate`, then
`pip install -e . --group dev` (pip >= 25.1 for `--group`) and
`pip install pre-commit`.

## Checks before pushing

- `pre-commit run --all-files`: ruff, ruff-format, mypy, prettier, codespell and
  more, at the versions pinned in `.pre-commit-config.yaml`. CI's Format job
  runs exactly this, so a locally installed ruff or mypy of another version is
  not a substitute.
- mypy runs in pre-commit's own environment, which has only `pytest`, `numpy`
  and `tomli`. An import of any other package (e.g. `xxhash`) needs
  `# type: ignore[import-not-found]`.
- `pytest`: the whole suite. `tests/test_spec_fixtures.py` and
  `tests/test_spec_cases.py` need the submodule and skip without it.
- `nox` runs both (its `lint` and `tests` sessions).

## Tests

- Every behaviour change gets a test that fails on `main` and passes with the
  change. Say in the PR which tests fail on `main`.
- Prefer real files to synthetic bytes: root-io-spec's fixtures first, then
  `scikit-hep-testdata`. When a `case.toml` pins bytes at an offset, assert
  those exact values.
- Name a new test module after what it tests (`test_page_checksums.py`, not
  `test_rntuple.py`). Older modules such as `test_read.py` predate this rule;
  leave their names alone.
- `EXPECTED_FAILURES` in `tests/test_spec_fixtures.py` lists every fixture that
  fails, with its tracking issue and exact error. A fix that changes what a
  fixture does must update its entry: remove it, or move it on to the next error
  and issue.

## Issues, commits and pull requests

- One issue per problem, with evidence: the file, the offset, the spec section,
  the error. Something new found while working on another issue gets its own
  issue, not a silent fix.
- File a new issue as a native sub-issue of its tracker: #66 for the bootstrap
  review, #9 for files that don't read yet.
- One pull request per issue, or per tight group of related issues. A PR built
  on another says **Depends on #N** and is rebased when that one merges.
- Commits are small. The message says what changed and why, with spec citations.
- A PR description has: `Fixes #N`; what changed and why, with citations; a
  **Tests** section (what each test pins, what fails on `main`, the full-suite
  count); and a **Breaking changes** list written for the release notes.
- PRs are squash-merged once the maintainer approves them.

## Marking AI-generated work

- Issues, PR descriptions and comments written by an agent start with
  `> 🤖 AI generated content`.
- Issue bodies and PR descriptions also end with
  `Assisted-by: <tool>:<model id>`, e.g.
  `Assisted-by: claude-code:claude-opus-5-5`.
- Commit messages end with the same `Assisted-by:` line.
- `Assisted-by:` replaces any tool's default attribution: no `Co-authored-by`
  trailer, and no "Generated with ..." footer.
