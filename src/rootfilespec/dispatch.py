from rootfilespec.structutil import ROOTSerializable

DICTIONARY: dict[str, type[ROOTSerializable]] = {}

# TODO: is this encoding correct?
ENCODING = "utf-8"


def normalize(s: bytes) -> str:
    """Convert the ROOT C++ class name to a representation that is valid in Python.

    This is used to generate the class name in the DICTIONARY.
    """
    out = s.decode(ENCODING)
    return (
        out.replace(":", "3a")
        .replace("<", "3c")
        .replace(">", "3e")
        .replace(",", "2c")
        .replace(" ", "_")
        .replace("const_", "")
    )
