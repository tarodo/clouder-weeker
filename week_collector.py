import logging

import requests
from environs import Env

import utils
from dbs_config import get_raw_db
from raw_adapter import (
    collect_bp_releases,
    collect_bp_tracks,
    collect_week_sp_tracks,
    load_bp_releases,
    load_bp_tracks,
    load_sp_track,
)
from spotify_adapter import add_tracks, create_playlist, create_sp, get_track_by_isrc

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("weeker")

BP_BASE_URL = "https://api.beatport.com"
RELEASES_URL = f"{BP_BASE_URL}/v4/catalog/releases"

BP_STYLES = {
    1: "DNB",
    90: "TECHNO",
}


class ReleaseMeta:
    def __init__(self, week: int, year: int, style_id: int):
        self.week = week
        self.year = year
        self.style_id = style_id
        self.week_start, self.week_end = utils.get_start_end_dates(self.year, self.week)
        self.style_name = BP_STYLES.get(self.style_id)
        self.playlist_name = f"{self.style_name} :: {self.year} :: {self.week}"


def save_one_page(url, params, headers):
    if not url.startswith("https://"):
        url = f"https://{url}"
    r = requests.get(url, params=params, headers=headers)
    r.raise_for_status()
    release_page = r.json()
    load_bp_releases(release_page["results"])
    next_page = release_page["next"]
    return next_page, dict()


def collect_week(release_meta: ReleaseMeta, bp_token: str) -> bool:
    url = f"{RELEASES_URL}/"
    params = {
        "genre_id": {release_meta.style_id},
        "publish_date": f"{release_meta.week_start}:{release_meta.week_end}",
        "page": 1,
        "per_page": 100,
        "order_by": "-publish_date",
    }
    headers = {"Authorization": f"Bearer {bp_token}"}
    while url:
        url, params = save_one_page(url, params, headers)
    return True


def handle_week(release_meta: ReleaseMeta, bp_token: str):
    logger.info(f"Start :: {release_meta}")
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
    load_bp_tracks(tracks["results"], mongo_db)


def collect_tracks(release_meta: ReleaseMeta, bp_token: str):
    release_ids = collect_bp_releases(
        release_meta.week_start.isoformat(), release_meta.week_end.isoformat()
    )
    mongo_db = get_raw_db()
    for release_id in release_ids:
        handle_one_release(release_id["id"], bp_token, mongo_db)


def collect_spotify_releases(release_meta: ReleaseMeta):
    sp = create_sp()

    not_found = []
    tracks_count = 0
    bp_tracks = collect_bp_tracks(
        release_meta.week_start.isoformat(), release_meta.week_end.isoformat()
    )
    mongo_db = get_raw_db()
    for bp_track in bp_tracks:
        sp_track = get_track_by_isrc(bp_track["isrc"], sp)
        if sp_track:
            sp_track.update(
                {
                    "bp_track_id": bp_track["id"],
                    "bp_year": release_meta.year,
                    "bp_week": release_meta.week,
                }
            )
            load_sp_track(sp_track, mongo_db)
            tracks_count += 1
        else:
            not_found.append(bp_track)

    logger.info(f"Not found : {not_found}")
    logger.info(f"Tracks count : {tracks_count}")
    logger.info(f"Not found count : {len(not_found)}")


def create_spotify_playlist(release_meta: ReleaseMeta):
    sp = create_sp()

    playlist_id, playlist_url = create_playlist(sp, release_meta.playlist_name)
    logger.info(f"Playlist : {release_meta.playlist_name} : created : {playlist_url}")
    week_tracks = collect_week_sp_tracks(release_meta.year, release_meta.week)
    tracks_ids = [track["id"] for track in week_tracks]

    logger.info(f"Tracks count ready to load : {len(tracks_ids)}")
    add_tracks(sp, playlist_id, tracks_ids)


if __name__ == "__main__":
    env = Env()
    env.read_env()
    bp_token = env.str("BP_TOKEN")
    release_attr = ReleaseMeta(week=11, year=2023, style_id=1)
    handle_week(release_attr, bp_token)
    collect_tracks(release_attr, bp_token)
    collect_spotify_releases(release_attr)
    create_spotify_playlist(release_attr)
