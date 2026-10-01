# AGENTS.md

Guidance for coding agents (and people) working on rootfilespec. Read
`README.md` and `docs/design.md` first.

## What this package is

rootfilespec parses ROOT file binary data into Python dataclasses of primitive
types and numpy arrays. Its parsing code does no I/O: it takes bytes buffers and
returns objects (`reader.py`, below, is the one place that reads files). The
goal is a stable, complete read (and later write) backend for packages such as
uproot.

- `src/rootfilespec/bootstrap/`: the self-describing part of a file (`TFile`,
  `TKey`, `TDirectory`, `TStreamerInfo`, strings, compression) and the RNTuple
  anchor.
- `src/rootfilespec/rntuple/`: RNTuple envelopes, schema and page locations.
- `src/rootfilespec/dynamic.py`: generates classes from a file's `TStreamerInfo`
  (see Design rules below for its direction, #67).
- `src/rootfilespec/reader.py`: `FileReader` and `Fetcher`, the I/O side.

## Design rules

`docs/design.md` has each rule in full, with its reasons and the issues still
open against it. In short:

- **Objects describe where data is; callers fetch.** Parsing code never reads
  from a file itself ([locators](docs/design.md#data-fetching-and-locators)).
- **A builtin only where the context records the ROOT type**: a member's
  annotation, a container's element type, a `TKey`, or a pointee's `Ref`. Its
  decoding must be injective. Every string, key names included, is `bytes`,
  never decoded ([builtins](docs/design.md#annotated-builtin-types-vs-objects)).
- **Keep what is on disk**: everything a writer needs to write the bytes back
  ([on disk](docs/design.md#keeping-what-is-on-disk)).
- **Don't guess**: raise a clear error or keep the bytes uninterpreted
  ([unknown content](docs/design.md#content-the-parser-does-not-understand)).
- **Generated classes**: don't build more on annotations and inheritance (#67)
  ([generated classes](docs/design.md#generated-classes)).

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
  case's `case.toml`, under `gen/cases/` or `gen/written/`, pins values at file
  offsets.

## Setup

```sh
git submodule update --init reference/root-io-spec   # never --recursive: it nests all of ROOT (~1.5 GB)
uv sync --group dev                                  # CI's test group, plus mypy
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
- `pytest`: the whole suite. Check out the submodule first: the tests that use
  its fixtures skip without it, and a test that doesn't is a bug.
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
- File a new issue as a native sub-issue of its tracker where one fits: #66 for
  the bootstrap review, #9 for files that don't read yet.
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
