from rootfilespec.structutil import ROOTSerializable

DICTIONARY: dict[str, type[ROOTSerializable]] = {}

# TODO: is this encoding correct?
ENCODING = "utf-8"


def normalize(s: bytes) -> str:
    out = s.decode(ENCODING)
    return (
        out.replace(":", "3a")
        .replace("<", "3c")
        .replace(">", "3e")
        .replace(",", "2c")
        .replace(" ", "_")
    )
