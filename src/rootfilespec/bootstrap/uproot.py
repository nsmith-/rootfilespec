from typing import Any

from uproot.model import Model  # type: ignore[import-not-found]

from rootfilespec.serializable import ROOTSerializable, _get_annotations


class UprootModelAdapter(Model):  # type: ignore[misc]
    """
    Adapter for Uproot models to be used with the rootfilespec library.
    This class allows Uproot to read ROOTSerializable objects
    """

    _model: ROOTSerializable
    _file: Any

    def __init__(self, model: Any) -> None:
        self._model = model

    def __getattr__(self, name: str):
        # Try the wrapped model first
        if hasattr(self._model, name):
            return getattr(self._model, name)

        # Then try the class we're adapting to (Uproot behavior/model internals)
        behavior_cls = getattr(type(self), "__behavior_cls__", None)
        if behavior_cls and hasattr(behavior_cls, name):
            return getattr(behavior_cls, name)

        # Fall back to error
        raise AttributeError(f"{self.__class__.__name__} has no attribute {name!r}")


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

    def member(self, name, all: bool = True, none_if_missing: bool = False):
        if all:
            value = getattr(self._model, name, None)
        else:
            # Check only declared fields on this type (not bases)
            fields = _get_annotations(type(self._model))
            value = getattr(self._model, name, None) if name in fields else None

        if none_if_missing and value is None:
            msg = f"Member {name} not found in {self._model.__class__.__name__}"
            raise AttributeError(msg)

        return self._adapt_value(value)

    def _adapt_value(self, value):
        """Normalize ROOTSerializable values into Uproot-friendly objects."""
        if isinstance(value, ROOTSerializable):
            if value.__class__.__name__ == "TString":
                return value.fString.decode("utf-8")

            adapter_cls = create_adapter_class(type(value))
            return adapter_cls(value)

        elif isinstance(value, (list, tuple)):
            return type(value)(self._adapt_value(v) for v in value)

        elif isinstance(value, dict):
            return {k: self._adapt_value(v) for k, v in value.items()}

        return value 


from typing import Any

def create_adapter_class(model_cls):
    """
    Wrap a Model class (like Model_TTree_v20) into an Adapter that exposes
    lookup, bases, cache_key, and underscored aliases safely.
    """
    try:
            behavior_subclasses = model_cls.behavior
    except AttributeError:
            behavior_subclasses = (
                base
                for base in model_cls.__bases__
                if "uproot.behavior" in base.__module__
            )
            print(model_cls.__bases__)

    # TODO: Check if we can use model_cls instead of *behavior_subclasses
    class Adapter(UprootModelAdapter, *behavior_subclasses):   
        def __init__(self, model_instance):
            super().__init__(model_instance)
            self._internal_lookup = getattr(model_instance, "_lookup", {})
            self._internal_bases = getattr(model_instance, "_bases", [])
            self._internal_cache_key = getattr(model_instance, "cache_key", None)

        @property
        def name(self):
            return getattr(self._model, "name", None)

        @property
        def class_version(self):
            return getattr(self._model, "class_version", None)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            try:
                close = getattr(self, "close", None)
                if callable(close):
                    close()
                else:
                    mclose = getattr(self._model, "close", None)
                    if callable(mclose):
                        mclose()
            except Exception:
                return False
            return False

        # def __getitem__(self, key):
        #     try:
        #         return self.lookup[key]
        #     except Exception:
        #         return self._internal_lookup[key]

        # @property
        # def lookup(self):
        #     val = getattr(self.model, "lookup", None)
        #     if val is None or isinstance(val, property):
        #         return self._internal_lookup
        #     return val

        @property
        def bases(self):
            val = getattr(self.model, "bases", None)
            if val is None or isinstance(val, property):
                return self._internal_bases
            return val

        @property
        def cache_key(self):
            val = getattr(self.model, "cache_key", None)
            if val is None or isinstance(val, property):
                return self._internal_cache_key
            return val

        # @property
        # def _lookup(self):
        #     return self.lookup

        @property
        def _bases(self):
            return self.bases

        @property
        def _cache_key(self):
            return self.cache_key

    Adapter.__name__ = f"Adapter_{model_cls.__name__}"

    return Adapter
