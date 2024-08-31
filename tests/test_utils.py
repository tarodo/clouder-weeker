import pytest
from datetime import datetime
from src.utils import get_start_end_dates

def test_get_start_end_dates_valid_week():
    # Test with a valid week number
    start_date, end_date = get_start_end_dates(2023, 1)
    assert start_date == datetime(2023, 1, 2).date()
    assert end_date == datetime(2023, 1, 8).date()

    start_date, end_date = get_start_end_dates(2023, 52)
    assert start_date == datetime(2023, 12, 25).date()
    assert end_date == datetime(2023, 12, 31).date()

def test_get_start_end_dates_invalid_week():
    # Test with an invalid week number that spills over to the next year
    with pytest.raises(ValueError):
        get_start_end_dates(2023, 53)

def test_get_start_end_dates_first_day_not_monday():
    # Test where the first day of the year is not a Monday
    start_date, end_date = get_start_end_dates(2024, 1)
    assert start_date == datetime(2024, 1, 1).date()
    assert end_date == datetime(2024, 1, 7).date()

def test_get_start_end_dates_last_week():
    # Test the last valid week of the year
    start_date, end_date = get_start_end_dates(2024, 52)
    assert start_date == datetime(2024, 12, 23).date()
    assert end_date == datetime(2024, 12, 29).date()

def test_get_start_end_dates_edge_case():
    # Test edge case where week_number is 0
    with pytest.raises(ValueError):
        get_start_end_dates(2023, 0)