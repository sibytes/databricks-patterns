import pytest

from dbxconfig import Timeslice, TimesliceNow, TimesliceUtcNow


# @pytest.fixture
# def spark_schema():
#     pass


def test_timeslice_all():

    format = "%Y%m%d"
    timeslice = Timeslice(day="*", month="*", year="*")
    actual = timeslice.strftime(format)
    expected = "***"
    assert actual == expected


def test_timeslice_year():

    format = "%Y/%m/%d"
    timeslice = Timeslice(day="*", month="*", year=2023)
    actual = timeslice.strftime(format)
    expected = "2023/*/*"
    assert actual == expected


def test_timeslice_month():

    format = "%Y-%m-%d"
    timeslice = Timeslice(day="*", month=1, year=2023)
    actual = timeslice.strftime(format)
    expected = "2023-01-*"
    assert actual == expected


def test_timeslice_day():

    format = "%Y\\%m\\%d"
    timeslice = Timeslice(day=1, month=1, year=2023)
    actual = timeslice.strftime(format)
    expected = "2023\\01\\01"
    assert actual == expected


def test_timeslice_month():

    format = "%Y-%m-%d"
    timeslice = Timeslice(day="*", month=1, year=2023)
    actual = timeslice.strftime(format)
    expected = "2023-01-*"
    assert actual == expected

