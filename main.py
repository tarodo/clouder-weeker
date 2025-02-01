import logging
import time

from environs import Env

from src.bp_adapter import collect_releases, handle_one_release
from src.common import ReleaseMeta
from src.logging_config import setup_logging
from src.mongo_adapter import (collect_bp_releases, collect_releases_tracks,
                               collect_sp_week_tracks, save_data_mongo_by_id)
from src.sp_adapter import (create_playlist, create_playlist_with_tracks,
                            get_track_by_isrc)

setup_logging()
logger = logging.getLogger("main")


def bp_release_processing(
    release_attr: ReleaseMeta, bp_url: str, bp_token: str
) -> None:
    """Collect and save BP releases, return a list of release IDs."""
    logger.info(f"Collect BP releases :: {release_attr.clouder_week} :: Start")
    release_cnt = 0
    for releases in collect_releases(release_attr, bp_url, bp_token):
        release_cnt += len(releases)
        for release in releases:
            release["clouder_week"] = release_attr.clouder_week
        save_data_mongo_by_id(releases, "bp_releases")

    release_attr.set_statistic("bp_releases", release_cnt)
    logger.info(f"Collect BP releases :: {release_attr.clouder_week} :: Done")


def bp_release_tracks_processing(
    release_attr: ReleaseMeta, bp_url: str, bp_token: str
) -> None:
    """Collect and save tracks for each BP release."""
    logger.info(f"Collect BP tracks :: {release_attr.clouder_week} :: Start")
    release_ids = collect_bp_releases(release_attr.clouder_week)
    track_cnt = 0
    for idx, release_id in enumerate(release_ids):
        logger.info(f"Handle release from bp : {idx + 1}/{len(release_ids)} :: Start")
        for tracks in handle_one_release(release_id, bp_url, bp_token):
            track_cnt += len(tracks)
            for track in tracks:
                track["clouder_week"] = release_attr.clouder_week
            save_data_mongo_by_id(tracks, "bp_tracks")
        logger.info(f"Handle release from bp : {idx + 1}/{len(release_ids)} :: Done")
    logger.info(f"Collect BP tracks :: {release_attr.clouder_week} :: Done")


def bp_tracks_processing(release_attr: ReleaseMeta, bp_url: str, bp_token: str) -> None:
    """Collect and save BP clear tracks."""
    logger.info(f"Collect BP clear tracks :: {release_attr.clouder_week} :: Start")
    for tracks in collect_releases(release_attr, bp_url, bp_token, "tracks"):
        for track in tracks:
            track["clouder_week"] = release_attr.clouder_week
        save_data_mongo_by_id(tracks, "bp_tracks")
    logger.info(f"Collect BP clear tracks :: {release_attr.clouder_week} :: Done")


def sp_tracks_pack_processing(
    bp_tracks: list[dict], clouder_week: str, is_genre: bool = True
) -> list:
    not_found = []
    sp_tracks = []

    for bp_track in bp_tracks:
        sp_track = get_track_by_isrc(bp_track["isrc"])
        if not sp_track:
            not_found.append(bp_track["id"])
            continue
        sp_track.pop("available_markets", None)
        sp_track["album"].pop("available_markets", None)
        sp_track["bp_id"] = bp_track["id"]
        sp_track["clouder_week"] = clouder_week
        sp_track["clouder_genre"] = is_genre
        sp_tracks.append(sp_track)

        if len(sp_tracks) >= 100:
            save_data_mongo_by_id(sp_tracks, "sp_tracks")
            sp_tracks.clear()
            time.sleep(60)

    if sp_tracks:
        save_data_mongo_by_id(sp_tracks, "sp_tracks")

    return not_found


def sp_tracks_processing(release_attr: ReleaseMeta) -> None:
    """Collect and save SP tracks based on BP tracks."""
    clouder_week = release_attr.clouder_week
    genre_tracks, not_genre_tracks = collect_releases_tracks(
        clouder_week, release_attr.style_id
    )

    release_attr.set_statistic("bp_genre_tracks", len(genre_tracks))
    release_attr.set_statistic("bp_not_genre_tracks", len(not_genre_tracks))
    full_count = len(genre_tracks) + len(not_genre_tracks)

    logger.info(f"Collect spotify tracks :: {clouder_week} :: BP {full_count} :: Start")

    not_found = sp_tracks_pack_processing(genre_tracks, clouder_week, True)
    not_found += sp_tracks_pack_processing(not_genre_tracks, clouder_week, False)

    if not_found:
        not_found_week = [
            {"id": clouder_week, "not_found": not_found},
        ]
        save_data_mongo_by_id(not_found_week, "not_found_sp_tracks")
        logger.warning(
            f"Spotify tracks not found :: {clouder_week} :: Count {len(not_found)}"
        )

    found_cnt = full_count - len(not_found)
    release_attr.set_statistic("sp_tracks", found_cnt)
    release_attr.set_statistic("sp_not_found", len(not_found))

    logger.info(
        f"Collect spotify tracks :: {clouder_week} :: BP {full_count} :: SP {found_cnt} :: Done"
    )


def processing_sp_playlists(release_attr: ReleaseMeta) -> None:
    """Create and fill SP playlists based on SP tracks of the week."""
    clouder_week = release_attr.clouder_week
    logger.info(f"Create spotify playlists :: {clouder_week} :: Start")

    new_tracks, old_tracks, not_genre_tracks = collect_sp_week_tracks(
        clouder_week, release_attr.week_start
    )

    new_playlist_name = release_attr.generate_sp_playlist_name("new")
    new_playlist_id = create_playlist_with_tracks(new_playlist_name, new_tracks)
    old_playlist_name = release_attr.generate_sp_playlist_name("old")
    old_playlist_id = create_playlist_with_tracks(old_playlist_name, old_tracks)
    not_genre_playlist_name = release_attr.generate_sp_playlist_name("not")
    not_genre_playlist_id = create_playlist_with_tracks(
        not_genre_playlist_name, not_genre_tracks
    )
    trash_playlist_name = release_attr.generate_sp_playlist_name("trash")
    trash_playlist_id = create_playlist(trash_playlist_name)

    release_attr.set_sp_playlist("new", new_playlist_name, new_playlist_id, "base")
    release_attr.set_sp_playlist("old", old_playlist_name, old_playlist_id, "base")
    release_attr.set_sp_playlist(
        "not", not_genre_playlist_name, not_genre_playlist_id, "base"
    )
    release_attr.set_sp_playlist("trash", trash_playlist_name, trash_playlist_id, "base")
    for pl in release_attr.extra_playlists:
        pl_name = release_attr.generate_sp_playlist_name(pl)
        pl_id = create_playlist(pl_name)
        release_attr.set_sp_playlist(pl, pl_name, pl_id, "extra")

    logger.info(f"Create spotify playlists :: {clouder_week} :: Done")


if __name__ == "__main__":
    env = Env()
    env.read_env()

    bp_url = env.str("BP_URL")
    bp_token = env.str("BP_TOKEN")

    release_meta = ReleaseMeta(week=1, year=2025, style_id=1)

    # bp_release_processing(release_meta, bp_url, bp_token)
    # bp_release_tracks_processing(release_meta, bp_url, bp_token)
    # bp_tracks_processing(release_meta, bp_url, bp_token)

    sp_tracks_processing(release_meta)
    save_data_mongo_by_id([release_meta.data_to_mongo()], "clouder_weeks")

    processing_sp_playlists(release_meta)
    save_data_mongo_by_id([release_meta.data_to_mongo()], "clouder_weeks")
