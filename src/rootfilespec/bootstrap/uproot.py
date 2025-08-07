from typing import Any

from uproot.model import Model  # type: ignore[import-not-found]

from rootfilespec.serializable import ROOTSerializable, _get_annotations


class UprootModelAdapter(Model):  # type: ignore[misc]
    """
    Adapter for Uproot models to be used with the rootfilespec library.
    This class allows Uproot to read ROOTSerializable objects
    """

    _model: ROOTSerializable

    def __init__(self, model: Any) -> None:
        self._model = model

    @property
    def _fields(self):
        return _get_annotations(type(self._model))

    @property
    def encoded_classname(self):
        name = type(self._model).__name__.replace("3a3a", "_3a3a_")
        return "Model_" + name

    def num_members(self):
        return len(self._fields)

    def member_name(self, index):
        try:
            return list(self._fields.keys())[index]
        except IndexError:
            err = f"Member index {index} out of range"
            raise IndexError(err) from None

    def member(self, name, all=True, none_if_missing=False):
        if all:
            out = getattr(self._model, name, None)
        else:
            # check the member is in this type and not base classes
            fields = _get_annotations(type(self._model))
            out = None if name not in fields else getattr(self._model, name, None)

        if none_if_missing and out is None:
            msg = f"Member {name} not found in {self._model.__class__.__name__}"
            raise AttributeError(msg)
        return out


def create_adapter_class(uproot_cls: Any) -> type:
    behavior_cls = getattr(uproot_cls, "__behavior__", object) or object

    def call_optional_init(init_func: Any, instance: Any) -> None:
        if callable(init_func) and init_func is not object.__init__:
            init_func(instance)

    class Adapter(UprootModelAdapter, behavior_cls):  # type: ignore[misc, valid-type]
        def __init__(self, model: Any) -> None:
            # Call known init directly — safe and type-checkable
            UprootModelAdapter.__init__(self, model)

            init = getattr(behavior_cls, "__init__", None)
            call_optional_init(init, self)

    Adapter.__name__ = f"Adapter_{behavior_cls.__name__}"
    return Adapter
