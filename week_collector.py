import json
import logging
import os
from pathlib import Path

import requests
from environs import Env

import week_dates
from raw_data import load_bp_releases, collect_bp_releases, load_bp_release_tracks, collect_bp_release_tracks, get_db, \
    load_sp_track
from spotify_collector import (add_tracks, create_playlist, create_sp,
                               get_track_by_isrc)


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("weeker")

BP_BASE_URL = "https://api.beatport.com"
RELEASES_URL = f"{BP_BASE_URL}/v4/catalog/releases"

BP_STYLES = {
    1: "DNB",
    90: "TECHNO",
}


class ReleaseMeta:
    DATA_DIR = "data"
    WEEK_RAW_DIR = "week_raw"
    TRACKS_RAW_DIR = "tracks_raw"
    TRACKS_SPOTIFY_DIR = "tracks_spotify"
    NOT_FOUND_FILE = "_not_found.json"

    def __init__(self, week: int, year: int, style_id: int):
        self.week = week
        self.year = year
        self.style_id = style_id

        self.week_start, self.week_end = week_dates.get_start_end_dates(self.year, self.week)
        self.style_name = BP_STYLES.get(self.style_id)
        self.releases_path = self._create_path(self.DATA_DIR,  self.style_name, f"{self.year}",
                                               str(self.week).zfill(2))
        self.week_raw_path = self._create_path(self.releases_path, self.WEEK_RAW_DIR)
        self.tracks_raw_path = self._create_path(self.releases_path, self.TRACKS_RAW_DIR)
        self.tracks_spotify_path = self._create_path(self.releases_path, self.TRACKS_SPOTIFY_DIR)
        self.not_found_path = self.tracks_spotify_path / self.NOT_FOUND_FILE

    def __str__(self):
        return (f"{self.style_name} :: {self.year} :: {self.week}\n{self.week_start} : {self.week_end}\n"
                f"{self.releases_path}\n{self.week_raw_path}\n{self.tracks_raw_path}\n{self.tracks_spotify_path}\n"
                f"{self.not_found_path}")

    @staticmethod
    def _create_path(*args):
        path = Path(*args)
        os.makedirs(path, exist_ok=True)
        return path


def save_one_page(url, params, headers, release_meta: ReleaseMeta):
    if not url.startswith("https://"):
        url = f"https://{url}"
    r = requests.get(url, params=params, headers=headers)
    r.raise_for_status()
    release_page = r.json()
    load_bp_releases(release_page["results"])
    next_page = release_page["next"]
    return next_page, dict()


def collect_week(release_meta: ReleaseMeta, bp_token: str) -> bool:
    url = RELEASES_URL
    params = {
        "genre_id": {release_meta.style_id},
        "publish_date": f"{release_meta.week_start}:{release_meta.week_end}",
        "page": 1,
        "per_page": 100,
        "order_by": "-publish_date",
    }
    headers = {"Authorization": f"Bearer {bp_token}"}
    while url:
        url, params = save_one_page(url, params, headers, release_meta)
    return True


def handle_week(release_meta: ReleaseMeta, bp_token: str):
    logger.info(
        f"Start :: {release_meta}"
    )
    collect_week(release_meta, bp_token)


def handle_one_release(release_id: int, bp_token: str, mongo_db=None):
    url = f"{RELEASES_URL}/{release_id}/tracks/"
    params = {
        "page": 1,
        "per_page": 100,
    }
    headers = {"Authorization": f"Bearer {bp_token}"}
    r = requests.get(url, params=params, headers=headers)
    r.raise_for_status()
    tracks = r.json()
    load_bp_release_tracks(tracks["results"], mongo_db)


def collect_tracks(release_meta: ReleaseMeta, bp_token: str):
    release_ids = collect_bp_releases(release_meta.week_start.isoformat(), release_meta.week_end.isoformat())
    mongo_db = get_db()
    for release_id in release_ids:
        handle_one_release(release_id["id"], bp_token, mongo_db)


def collect_spotify_releases(release_meta: ReleaseMeta):
    sp = create_sp()

    not_found = []
    tracks_count = 0
    bp_tracks = collect_bp_release_tracks(release_meta.week_start.isoformat(), release_meta.week_end.isoformat())
    mongo_db = get_db()
    for bp_track in bp_tracks:
        sp_track = get_track_by_isrc(bp_track["isrc"], sp)
        if sp_track:
            sp_track.update({"bp_track_id": bp_track["id"]})
            load_sp_track(sp_track, mongo_db)
            tracks_count += 1
        else:
            not_found.append(bp_track)

    logger.info(f"Not found : {not_found}")
    logger.info(f"Tracks count : {tracks_count}")
    logger.info(f"Not found count : {len(not_found)}")


def create_spotify_playlist(year: int, week_num: int, style_id: int):
    week_num = str(week_num).zfill(2)
    style_name = BP_STYLES.get(style_id)
    releases_path = Path(DATA_DIR) / style_name / f"{year}" / week_num
    spotify_tracks_path = releases_path / TRACKS_SPOTIFY_DIR

    sp = create_sp()

    playlist_name = f"{style_name} :: {year} :: {week_num}"
    playlist_id, playlist_url = create_playlist(sp, playlist_name)
    logger.info(f"Playlist : {playlist_name} : created : {playlist_url}")
    tracks_ids = []
    for release_file in spotify_tracks_path.iterdir():
        if release_file.name == NOT_FOUND_FILE:
            continue
        release_sp_tracks = json.load(open(release_file, "r"))

        tracks_ids.extend([track["id"] for track in release_sp_tracks])
    logger.info(f"Tracks count ready to load : {len(tracks_ids)}")
    add_tracks(sp, playlist_id, tracks_ids)


if __name__ == "__main__":
    env = Env()
    env.read_env()
    bp_token = env.str("BP_TOKEN")
    release_attr = ReleaseMeta(week=10, year=2023, style_id=1)
    # handle_week(release_attr, bp_token)
    # collect_tracks(release_attr, bp_token)
    collect_spotify_releases(release_attr)
    # create_spotify_playlist(year, week_num, style_id)
