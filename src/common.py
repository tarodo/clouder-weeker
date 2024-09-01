from src.utils import get_start_end_dates

BP_STYLES = {
    1: "DNB",
    90: "TECHNO",
}

class ReleaseMeta:
    def __init__(self, week: int, year: int, style_id: int):
        self.week = week
        self.year = year
        self.style_id = style_id

        # Calculate meta additional fields
        self.week_start, self.week_end = get_start_end_dates(self.year, self.week)
        self.style_name = BP_STYLES.get(self.style_id)
        self.playlist_name = f"{self.style_name} :: {self.year} :: {self.week}"

    def __str__(self):
        return f"{self.style_name} :: {self.year} :: {self.week}"