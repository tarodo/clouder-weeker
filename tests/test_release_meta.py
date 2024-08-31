from datetime import datetime
from src.common import ReleaseMeta, BP_STYLES


def test_release_meta_initialization():
    # Test the initialization of ReleaseMeta with valid inputs
    release = ReleaseMeta(week=1, year=2023, style_id=1)

    # Verify that attributes are correctly initialized
    assert release.week == 1
    assert release.year == 2023
    assert release.style_id == 1
    assert release.week_start == datetime(2023, 1, 2).date()
    assert release.week_end == datetime(2023, 1, 8).date()
    assert release.style_name == BP_STYLES[1]
    assert release.playlist_name == "DNB :: 2023 :: 1"


def test_release_meta_invalid_style_id():
    # Test ReleaseMeta with an invalid style_id
    release = ReleaseMeta(week=1, year=2023, style_id=999)

    # Verify that style_name is None and playlist_name handles it properly
    assert release.style_name is None
    assert release.playlist_name == "None :: 2023 :: 1"


def test_release_meta_str_method():
    # Test the __str__ method of ReleaseMeta
    release = ReleaseMeta(week=1, year=2023, style_id=1)

    # Verify that the string representation is correct
    assert str(release) == "DNB :: 2023 :: 1"


def test_release_meta_edge_case():
    # Test ReleaseMeta with an edge case (last week of the year)
    release = ReleaseMeta(week=52, year=2023, style_id=90)

    # Verify that the start and end dates and other attributes are correct
    assert release.week_start == datetime(2023, 12, 25).date()
    assert release.week_end == datetime(2023, 12, 31).date()
    assert release.style_name == "TECHNO"
    assert release.playlist_name == "TECHNO :: 2023 :: 52"