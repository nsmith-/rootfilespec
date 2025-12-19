import dataclasses
import sys
import types
import warnings

from rootfilespec import bootstrap, generated
from rootfilespec.bootstrap.TStreamerInfo import (
    ClassDef,
    TStreamerInfo,
)
from rootfilespec.dispatch import normalize
from rootfilespec.serializable import FileContext, ROOTSerializable

BOOTSTRAP_TYPES: set[str] = set(bootstrap.__all__) | {"ROOTSerializable"}


def streamerinfo_to_classes(streamerinfo: bootstrap.TList) -> str:
    lines: list[str] = []

    classes: dict[str, ClassDef] = {}
    declared: set[str] = set(BOOTSTRAP_TYPES)

    for item in streamerinfo.items:
        if not isinstance(item, TStreamerInfo):
            continue
        clsname = normalize(item.fName.fString)
        if clsname in declared:
            lines.append(f"# Class {clsname} already declared, skipping")
            lines.append(f"# Definition: {item}\n")
            continue
        classdef = item.class_definition()
        classes[classdef.name] = classdef

    # Collect all warnings for users here since the stack level
    # is not predictable in the recursive write function
    warning_log: list[str] = []

    # Write out in dependency order
    def write(classdef: ClassDef):
        for dep in classdef.dependencies:
            depdef = classes.pop(dep, None)
            if depdef is not None:
                write(depdef)
            elif dep not in declared:
                if dep == classdef.name:
                    msg = f"Class {classdef.name} depends on itself, which is not allowed (likely an unimplemented container type)"
                else:
                    msg = f"Class {classdef.name} depends on {dep} which is missing"
                warning_log.append("    " + msg)
                lines.append(f"class {dep}(Uninterpreted):\n    pass\n")
                declared.add(dep)
        lines.append(classdef.code)
        declared.add(classdef.name)

    while classes:
        _, classdef = classes.popitem()
        write(classdef)

    # Write out the warnings
    if warning_log:
        msg = "Errors were found in the StreamerInfo:\n"
        msg += "\n".join(warning_log)
        msg += "\nThese members will be uninterpreted and skipped."
        warnings.warn(msg, UserWarning, stacklevel=2)

    return "\n".join(lines)


@dataclasses.dataclass
class DynamicFileContext(FileContext):
    streamerinfo: bootstrap.TList
    module: types.ModuleType

    def __repr__(self) -> str:
        return f"<DynamicFileContext(module={self.module.__name__})>"

    def type_by_name(
        self, name: str, expect_version: int | None = None
    ) -> type[ROOTSerializable]:
        cls = self.module.__dict__.get(name)
        if cls is None:
            if name == "TLeafI":
                msg = "TLeafI not declared in StreamerInfo, e.g. uproot-issue413.root"
                # (84 other test files have it, e.g. uproot-issue121.root)
                # https://github.com/scikit-hep/uproot3/issues/413
                # Likely groot-v0.21.0 (Go ROOT file implementation) did not write the streamers for TLeaf
                raise NotImplementedError(msg)
            if name == "RooRealVar":
                msg = "RooRealVar not declared in the StreamerInfo, e.g. uproot-issue49.root"
                raise NotImplementedError(msg)
            if name in ("CalibrationCoefficient", "StIOEvent", "MGTRun", "TTime"):
                # Missing streamers for some example files
                # uproot-issue-861.root, uproot-issue-418.root, uproot-issue-607.root
                msg = f"Unknown type {name} (version {expect_version}): not found in {self}"
                raise NotImplementedError(msg)
            if name == "TMatrixTSym3cdouble3e":
                # Missing derived class streamer in uproot-issue-359.root
                # The base class TMatrixTBase3cdouble3e is present, however
                msg = "Missing derived class streamer for TMatrixTSym3cdouble3e, e.g. uproot-issue-359.root"
                raise NotImplementedError(msg)
            # TODO: try to see if it is in BOOTSTRAP_CONTEXT ?
            msg = f"Unknown type {name} (version {expect_version}): not found in {self}"
            raise ValueError(msg)
        return cls  # type: ignore[no-any-return]

    def type_by_checksum(self, checksum: bytes) -> type[ROOTSerializable]:
        return super().type_by_checksum(checksum)

    def purge_module(self) -> None:
        """Purge the dynamic module from sys.modules that this context refers to"""
        del sys.modules[self.module.__name__]


def build_file_context(streamerinfo: bootstrap.TList) -> DynamicFileContext:
    # First, calculate a hash of the streamerinfo to define a unique context name
    # The list should have all TStreamerInfo objects, which have checksums,
    # except for one TList of schema evolution data, which we use the dataclass repr
    checksums: list[int] = []
    for item in streamerinfo.items:
        if isinstance(item, TStreamerInfo):
            checksums.append(item.fCheckSum)
        elif isinstance(item, bootstrap.TList):
            checksums.append(hash(repr(item)))
    # TODO: hash is not consistent across processes. Does it matter?
    context_id = hash(tuple(checksums)) & sys.maxsize
    module_name = f"rootfilespec.generated.{context_id:016x}"

    # Now, render streamer info into dataclass definitions and exec them
    if module_name not in sys.modules:
        classes = streamerinfo_to_classes(streamerinfo)
        module = types.ModuleType(module_name)
        sys.modules[module.__name__] = module
        module.__dict__.update(
            {k: v for k, v in generated.__dict__.items() if not k.startswith("__")}
        )
        try:
            exec(classes, module.__dict__)
        except:
            # If there is an error in the classes, we want to remove the module
            # from sys.modules so it can be retried later
            del sys.modules[module.__name__]
            raise

    return DynamicFileContext(streamerinfo, sys.modules[module_name])
