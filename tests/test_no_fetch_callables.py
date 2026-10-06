"""No parsing code takes a way to fetch data (#114)

Objects describe where data is, and callers fetch (docs/design.md, *Data
Fetching and Locators*). So no public function or method of
rootfilespec.bootstrap or rootfilespec.rntuple takes a callable: the old
DataFetcher methods (get_TFile, read_object, ...) and the locator-to-buffer
ones (get_header, from_anchor, ...) all did.
"""

import importlib
import inspect
import pkgutil
from collections.abc import Callable, Iterator
from types import ModuleType

import rootfilespec.bootstrap
import rootfilespec.rntuple


def _modules() -> Iterator[ModuleType]:
    for package in (rootfilespec.bootstrap, rootfilespec.rntuple):
        yield package
        for info in pkgutil.iter_modules(package.__path__, package.__name__ + "."):
            yield importlib.import_module(info.name)


def _public_functions() -> Iterator[tuple[str, Callable[..., object]]]:
    for module in _modules():
        for name, obj in vars(module).items():
            if (
                name.startswith("_")
                or getattr(obj, "__module__", None) != module.__name__
            ):
                continue
            if inspect.isfunction(obj):
                yield f"{obj.__module__}.{name}", obj
            elif inspect.isclass(obj):
                for member_name, member in vars(obj).items():
                    if member_name.startswith("_"):
                        continue
                    function = (
                        member.__func__
                        if isinstance(member, classmethod | staticmethod)
                        else member
                    )
                    if inspect.isfunction(function):
                        yield (
                            f"{obj.__module__}.{obj.__qualname__}.{member_name}",
                            function,
                        )


def _takes_callable(function: Callable[..., object]) -> bool:
    for parameter in inspect.signature(function).parameters.values():
        annotation = parameter.annotation
        text = annotation if isinstance(annotation, str) else repr(annotation)
        if "Callable" in text or "fetch" in parameter.name.lower():
            return True
    return False


def test_scan_finds_methods():
    """The scan sees the methods it is meant to check"""
    names = {name for name, _ in _public_functions()}
    assert "rootfilespec.bootstrap.TKey.TKey.read_from" in names
    assert "rootfilespec.rntuple.RNTuple.RNTuple.from_envelopes" in names
    assert "rootfilespec.rntuple.envelope.REnvelopeLink.envelope_locator" in names


def test_no_public_method_takes_a_fetch_callable():
    offenders = sorted(
        name for name, function in _public_functions() if _takes_callable(function)
    )
    assert offenders == []
