import logging

import requests
from environs import Env

import utils
from dbs_config import get_raw_db, get_trans_session
from logging_config import setup_logging
from raw_adapter import (collect_bp_releases, collect_bp_tracks,
                         collect_week_sp_tracks, load_bp_releases,
                         load_bp_track, load_sp_track)
from spotify_adapter import (add_tracks, create_playlist, create_sp,
                             get_track_by_isrc)
from trans_adapter import (collect_week_tacks, connect_track_sp_to_bp,
                           reg_bp_track)

setup_logging()
logger = logging.getLogger()

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

        # Calculate meta additional fields
        self.week_start, self.week_end = utils.get_start_end_dates(self.year, self.week)
        self.style_name = BP_STYLES.get(self.style_id)
        self.playlist_name = f"{self.style_name} :: {self.year} :: {self.week}"

    def __str__(self):
        return f"{self.style_name} :: {self.year} :: {self.week}"


def save_one_page(url, params, headers):
    logger.info(f"Collecting page : {url} :: Start")
    if not url.startswith("https://"):
        url = f"https://{url}"
    r = requests.get(url, params=params, headers=headers)
    r.raise_for_status()
    release_page = r.json()
    load_bp_releases(release_page["results"])
    next_page = release_page["next"]
    logger.info(f"Collecting page : {url} :: Done")
    return next_page, dict()


def collect_week(release_meta: ReleaseMeta, bp_token: str) -> bool:
    logger.info(f"Collecting week : {release_meta} :: Start")
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
    logger.info(f"Collecting week : {release_meta} :: Done")
    return True


def handle_one_release(release_id: int, bp_token: str):
    logger.info(f"Handle release :: {release_id} :: Start")
    url = f"{RELEASES_URL}/{release_id}/tracks/"
    params = {
        "page": 1,
        "per_page": 100,
    }
    headers = {"Authorization": f"Bearer {bp_token}"}
    r = requests.get(url, params=params, headers=headers)
    r.raise_for_status()
    tracks = r.json()
    mongo_db = get_raw_db()
    for track in tracks["results"]:
        load_bp_track(track, mongo_db)

    session = get_trans_session()
    for track in tracks["results"]:
        reg_bp_track(track, session=session)
    session.close()
    logger.info(f"Handle release :: {release_id} :: Done")


def collect_tracks(release_meta: ReleaseMeta, bp_token: str):
    logger.info(f"Collect tracks for release :: {release_meta} :: Start")
    release_pack = collect_bp_releases(
        release_meta.week_start.isoformat(), release_meta.week_end.isoformat()
    )

    for one_release in release_pack:
        handle_one_release(one_release["id"], bp_token)
    logger.info(f"Collect tracks for release :: {release_meta} :: Done")


def collect_spotify_tracks(release_meta: ReleaseMeta):
    logger.info(f"Collect spotify tracks :: {release_meta} :: Start")
    sp = create_sp()

    not_found = []
    tracks_count = 0
    bp_tracks = collect_week_tacks(
        release_meta.week_start.isoformat(), release_meta.week_end.isoformat()
    )
    print(len(bp_tracks))
    print(bp_tracks[0])

    # mongo_db = get_raw_db()
    # for bp_track in bp_tracks:
    #     sp_track = get_track_by_isrc(bp_track["isrc"], sp)
    #     if sp_track:
    #         load_sp_track(sp_track, mongo_db)
    #
    #         connect_track_sp_to_bp(bp_track["id"], sp_track)
    #
    #         tracks_count += 1
    #     else:
    #         not_found.append(bp_track)

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
    release_attr = ReleaseMeta(week=10, year=2023, style_id=1)
    logger.info(f"Start handle week : {release_attr} :: Start")
    # collect_week(release_attr, bp_token)
    # collect_tracks(release_attr, bp_token)
    collect_spotify_tracks(release_attr)
    # create_spotify_playlist(release_attr)
    logger.info(f"Start handle week : {release_attr} :: Done")
