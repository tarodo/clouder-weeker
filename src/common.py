from typing import Dict

from src.utils import get_start_end_dates

BP_STYLES = {
    1: "DNB",
    90: "TECHNO",
}

PLAYLISTS = {
    "DNB": ["Melodic", "Eastern", "Hard", "Melancholy", "Party", "ReDrum"],
    "TECHNO": ["Mid", "Eastern", "House", "Low", "High"],
}


class ReleaseMeta:
    def __init__(self, week: int, year: int, style_id: int):
        self._week = week
        self._year = year
        self._style_id = style_id

        self._week_start, self._week_end = get_start_end_dates(self._year, self._week)

        self._statistic = {
            "bp_releases": None,
            "bp_tracks": None,
            "sp_tracks": None,
            "sp_not_found": None,
        }
        self._base_playlists = ["New", "Old"]

        self._sp_playlists = {self.generate_sp_playlist_name(pl): None for pl in self._playlists_names}

    @property
    def style_name(self) -> str:
        """Returns the style name based on the style_id."""
        return BP_STYLES.get(self._style_id)

    @property
    def _playlists_names(self) -> list[str]:
        return self._base_playlists + PLAYLISTS[self.style_name]

    @property
    def _base_sp_pl_name(self) -> str:
        return f"{self.style_name} :: {self._year} :: {self._week}"

    def generate_sp_playlist_name(self, pl_name: str) -> str:
        return f"{self._base_sp_pl_name} :: {pl_name.upper()}"

    def set_sp_playlist(self, pl_name: str, sp_id: str) -> None:
        self._sp_playlists[self.generate_sp_playlist_name(pl_name)] = sp_id

    def set_statistic(self, key: str, value: int) -> None:
        self._statistic[key] = value

    @property
    def clouder_week(self) -> str:
        return f"{self.style_name}_{self._year}_{self._week}"

    @property
    def sp_playlists(self) -> dict[str, str | None]:
        return self._sp_playlists


    def __str__(self):
        return f"{self.style_name} :: {self._year} :: {self._week}"

    def data_to_mongo(self):
        return {
            "week": self._week,
            "year": self._year,
            "style": self.style_name,
            "week_start": self._week_start.isoformat(),
            "week_end": self._week_end.isoformat(),
            "id": self.clouder_week,
            "sp_playlists": self.sp_playlists,
            "statistic": self._statistic
        }
