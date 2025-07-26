from uproot.model import Model

from rootfilespec.serializable import ROOTSerializable, _get_annotations


class UprootModelAdapter(Model):
    """
    Adapter for Uproot models to be used with the rootfilespec library.
    This class allows Uproot to read ROOTSerializable objects
    """
    _model: ROOTSerializable

    @property
    def encoded_classname(self):

        name = type(self._model).__name__.replace("3a3a", "_3a3a_")
        return "Model_" + name

    def __init__(self, model):
        self._model = model

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