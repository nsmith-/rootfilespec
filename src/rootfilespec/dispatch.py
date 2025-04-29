from rootfilespec.serializable import ROOTSerializable

DICTIONARY: dict[str, type[ROOTSerializable]] = {}

# TODO: is this encoding correct?
ENCODING = "utf-8"


def normalize(s: bytes) -> str:
    """Convert the ROOT C++ class name to a representation that is valid in Python.

    This is used to generate the class name in the DICTIONARY.
    """
    # TODO: #22 append version to all class names
    return (
        s.decode(ENCODING)
        .replace(":", "3a")
        .replace("<", "3c")
        .replace(">", "3e")
        .replace(",", "2c")
        .replace(" ", "20")
        .replace("*", "2a")
    )
