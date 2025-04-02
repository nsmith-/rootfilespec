from __future__ import annotations

from rootfilespec.structutil import ROOTSerializable

DICTIONARY: dict[str, type[ROOTSerializable]] = {}

# TODO: is this encoding correct?
ENCODING = "utf-8"


def normalize(s: bytes) -> str:
    out = s.decode(ENCODING)
    return out.replace(":", "3a")
