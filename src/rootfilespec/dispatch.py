from rootfilespec.serializable import ROOTSerializable

DICTIONARY: dict[str, type[ROOTSerializable]] = {}

# TODO: is this encoding correct?
ENCODING = "utf-8"


def normalize(s: bytes) -> str:
    """Convert the ROOT C++ class name to a representation that is valid in Python.

    This is used to generate the class name in the DICTIONARY.
    """
    return (
        s.decode(ENCODING)
        .replace(":", "3a")
        .replace("<", "3c")
        .replace(">", "3e")
        .replace(",", "2c")
        .replace(" ", "_")
        .replace("*", "2a")
    )


def pyclass_to_cppname(pyclass: str) -> bytes:
    """Convert the Python class name to a ROOT C++ class name."""
    return (
        pyclass.replace("*", "2a")
        .replace(" ", "_")
        .replace(",", "2c")
        .replace(">", "3e")
        .replace("<", "3c")
        .replace(":", "3a")
        .encode(ENCODING)
    )
