from datetime import datetime
from typing import Annotated

from rootfilespec.structutil import Fmt


def TDatime_to_datetime(fDatime: int) -> datetime | None:
    """Convert fDatime to datetime

    Using the ROOT file convention
    (year-1995)<<26|month<<22|day<<17|hour<<12|minute<<6|second

    ROOT does not validate the fields when it writes them (root-io-spec
    Record §3.7), so a stored value need not be a calendar date: ``0`` has
    month 0 and day 0, for example.

    Args:
        fDatime (int): fDatime value

    Returns:
        datetime: naive datetime in the writer's local time (the zone is not
        stored), or None if the fields are not a valid date and time
    """
    try:
        return datetime(
            year=(fDatime >> 26) + 1995,
            month=(fDatime >> 22) & 0xF,
            day=(fDatime >> 17) & 0x1F,
            hour=(fDatime >> 12) & 0x1F,
            minute=(fDatime >> 6) & 0x3F,
            second=(fDatime & 0x3F),
        )
    except ValueError:
        return None


# TODO: convert to datetime through a MemberSerDe
TDatime = Annotated[int, Fmt(">I")]
