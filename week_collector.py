import json
import logging
import os
from pathlib import Path

import requests
from environs import Env

import week_dates
from spotify_collector import get_track_by_isrc, create_sp, create_playlist, add_tracks

DATA_DIR = "data"
FULL_WEEK_FILE = "full_week.json"
WEEK_RAW_DIR = "week_raw"
TRACKS_RAW_DIR = "tracks_raw"
TRACKS_SPOTIFY_DIR = "tracks_spotify"
NOT_FOUND_FILE = "_not_found.json"

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("weeker")

BP_BASE_URL = "https://api.beatport.com"
RELEASES_URL = f"{BP_BASE_URL}/v4/catalog/releases"

BP_STYLES = {
    1: "DNB",
}


def save_one_page(url, params, headers, data_dir):
    logger.info(f"Try get: {url=}")
    if not url.startswith("https://"):
        url = f"https://{url}"
    r = requests.get(url, params=params, headers=headers)
    r.raise_for_status()
    release_page = r.json()
    page_num = release_page["page"].split("/")[0]
    page_path = data_dir / f"page_{page_num}.json"
    with open(page_path, "w") as f:
        json.dump(r.json(), f, indent=4)
    next_page = release_page["next"]
    return next_page, dict()


def collect_week(
    start_date: str, end_date: str, genre_id: int, bp_token: str, data_dir: Path
) -> list:
    url = RELEASES_URL
    params = {
        "genre_id": {genre_id},
        "publish_date": f"{start_date}:{end_date}",
        "page": 1,
        "per_page": 100,
        "order_by": "-publish_date",
    }
    headers = {"Authorization": f"Bearer {bp_token}"}
    data_dir = data_dir / WEEK_RAW_DIR
    os.makedirs(data_dir, exist_ok=True)
    while url:
        url, params = save_one_page(url, params, headers, data_dir)


def handle_week(year: int, week_number: int, style_id: int, bp_token: str):
    week_number = str(week_number).zfill(2)
    style_name = BP_STYLES.get(style_id)
    start, end = week_dates.get_start_end_dates(year, week_number)
    data_path = Path(DATA_DIR) / style_name / f"{year}" / week_number
    os.makedirs(data_path, exist_ok=True)

    logger.info(
        f"Start :: {style_name} :: Year : {year} Week : {week_number} :: {start} : {end}"
    )
    collect_week(start, end, style_id, bp_token, data_path)


def collect_essential_week(raw_dir_path: Path):
    releases = []
    for week_page in raw_dir_path.iterdir():
        with open(week_page, "r") as f:
            data = json.load(f)

        for item in data["results"]:
            artists = item["artists"]
            release_artists = []
            for artist in artists:
                release_artist = {
                    "id": artist["id"],
                    "name": artist["name"],
                    "url": artist["url"],
                }
                release_artists.append(release_artist)

            remixers = item["remixers"]
            release_remixers = []
            for remixer in remixers:
                release_remixer = {
                    "id": remixer["id"],
                    "name": remixer["name"],
                    "url": remixer["url"],
                }
                release_remixers.append(release_remixer)
            release = {
                "artists": release_artists,
                "remixers": release_remixers,
                "catalog_number": item["catalog_number"],
                "new_release_date": item["new_release_date"],
                "publish_date": item["publish_date"],
                "updated": item["updated"],
                "upc": item["upc"],
                "label_name": item["label"]["name"],
                "label_id": item["label"]["id"],
                "desc": item["desc"],
                "release_id": item["id"],
                "release_name": item["name"],
                "release_url": item["url"],
                "track_count": item["track_count"],
            }
            releases.append(release)
    return releases


def handle_one_release(release: dict, raw_tracks_path: Path, bp_token: str):
    url = f"{RELEASES_URL}/{release.get('release_id')}/tracks/"
    params = {
        "page": 1,
        "per_page": 100,
    }
    headers = {"Authorization": f"Bearer {bp_token}"}
    r = requests.get(url, params=params, headers=headers)
    r.raise_for_status()
    tracks = r.json()
    with open(f"{raw_tracks_path}/{release.get('release_id')}.json", "w") as f:
        json.dump(tracks, f, indent=4)


def handle_week_tracks(full_week_path: Path, raw_tracks_path: Path, bp_token: str):
    with open(full_week_path, "r") as f:
        releases = json.load(f)

    for release in releases:
        handle_one_release(release, raw_tracks_path, bp_token)


def collect_tracks_info(raw_tracks_path: Path):
    tracks = []
    for release_file in raw_tracks_path.iterdir():
        with open(release_file, "r") as f:
            data = json.load(f)
        for item in data["results"]:
            track = {
                "artists": item["artists"],
                "id": item["id"],
                "isrc": item["isrc"],
                "mix_name": item["mix_name"],
                "name": item["name"],
                "new_release_date": item["new_release_date"],
                "publish_date": item["publish_date"],
            }
            if item["genre"]["id"] != 1:
                continue
            tracks.append(track)
    return tracks


def collect_tracks(year: int, week_num: int, style_id: int, bp_token: str):
    week_num = str(week_num).zfill(2)
    style_name = BP_STYLES.get(style_id)

    releases_path = Path(DATA_DIR) / style_name / f"{year}" / week_num
    raw_dir_path = releases_path / WEEK_RAW_DIR
    full_week_path = releases_path / FULL_WEEK_FILE

    raw_tracks_path = releases_path / TRACKS_RAW_DIR
    os.makedirs(raw_tracks_path, exist_ok=True)

    releases = collect_essential_week(raw_dir_path)
    with open(full_week_path, "w") as f:
        json.dump(releases, f, indent=4)

    handle_week_tracks(full_week_path, raw_tracks_path, bp_token)
    tracks = collect_tracks_info(raw_tracks_path)


def collect_spotify_releases(year: int, week_num: int, style_id: int):
    week_num = str(week_num).zfill(2)
    style_name = BP_STYLES.get(style_id)
    releases_path = Path(DATA_DIR) / style_name / f"{year}" / week_num
    raw_tracks_path = releases_path / TRACKS_RAW_DIR

    spotify_tracks_path = releases_path / TRACKS_SPOTIFY_DIR
    os.makedirs(spotify_tracks_path, exist_ok=True)
    not_found_path = spotify_tracks_path / NOT_FOUND_FILE

    sp = create_sp()

    not_found = []
    tracks_count = 0
    for release_file in raw_tracks_path.iterdir():
        release_sp_tracks = []
        sp_release_path = spotify_tracks_path / release_file.name
        release_data = json.load(open(release_file, "r"))
        for track in release_data["results"]:
            sp_track = get_track_by_isrc(track["isrc"], sp)
            if sp_track:
                release_sp_tracks.append(sp_track)
                tracks_count += 1
            else:
                not_found.append(f"Track : {track['name']} : not found with isrc : {track['isrc']}")
        with open(sp_release_path, "w") as f:
            json.dump(release_sp_tracks, f, indent=4)
    if not_found:
        with open(not_found_path, "w") as f:
            json.dump(not_found, f, indent=4)
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
        release_sp_tracks = json.load(open(release_file, "r"))
        tracks_ids.extend([track["id"] for track in release_sp_tracks])
    logger.info(f"Tracks count ready to load : {len(tracks_ids)}")
    add_tracks(sp, playlist_id, tracks_ids)


if __name__ == "__main__":
    env = Env()
    env.read_env()
    bp_token = env.str("BP_TOKEN")
    week_num = 11
    year = 2023
    style_id = 1
    handle_week(year, week_num, style_id, bp_token)
    collect_tracks(year, week_num, style_id, bp_token)
    collect_spotify_releases(year, week_num, style_id)
    create_spotify_playlist(year, week_num, style_id)