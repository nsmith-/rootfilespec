from datetime import datetime

from rootfilespec.bootstrap.TDatime import TDatime_to_datetime


def _pack(year: int, month: int, day: int, hour: int, minute: int, second: int):
    return (
        (year - 1995) << 26
        | month << 22
        | day << 17
        | hour << 12
        | minute << 6
        | second
    )


def test_valid_datime():
    """root-io-spec Record §3.7: the packed fields decode to the local time"""
    value = _pack(2026, 9, 30, 14, 5, 59)
    assert TDatime_to_datetime(value) == datetime(2026, 9, 30, 14, 5, 59)


def test_invalid_datime_is_none():
    """Issue #87: ROOT does not validate the fields, so a stored value need not be
    a date; month 0 / day 0 (fDatime == 0) and month 13 give None, not ValueError"""
    assert TDatime_to_datetime(0) is None
    assert TDatime_to_datetime(_pack(2026, 13, 1, 0, 0, 0)) is None
    assert TDatime_to_datetime(_pack(2026, 2, 30, 0, 0, 0)) is None
