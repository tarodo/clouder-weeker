import logging

from src.bp_adapter import handle_one_release, collect_releases
from src.common import ReleaseMeta
from src.logging_config import setup_logging
from environs import Env

from src.mongo_adapter import collect_bp_releases, save_data_mongo_by_id, collect_releases_tracks, \
    collect_sp_week_tracks
from src.sp_adapter import get_track_by_isrc, create_playlist_with_tracks, create_playlist

setup_logging()
logger = logging.getLogger("main")


def bp_release_processing(release_attr: ReleaseMeta, bp_url: str, bp_token: str) -> list[int]:
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
    return collect_bp_releases(release_attr.week_start.isoformat(), release_attr.week_end.isoformat())


def bp_tracks_processing(release_attr: ReleaseMeta, release_ids: list, bp_url: str, bp_token: str) -> list[dict]:
    """Collect and save tracks for each BP release."""
    logger.info(f"Collect BP tracks :: {release_attr.clouder_week} :: Start")
    track_cnt = 0
    for idx, release_id in enumerate(release_ids):
        logger.info(f"Handle release from bp : {idx + 1}/{len(release_ids)} :: Start")
        for tracks in handle_one_release(release_id, bp_url, bp_token):
            track_cnt += len(tracks)
            for track in tracks:
                track["clouder_week"] = release_attr.clouder_week
            save_data_mongo_by_id(tracks, "bp_tracks")
        logger.info(f"Handle release from bp : {idx + 1}/{len(release_ids)} :: Done")
    release_attr.set_statistic("bp_tracks", track_cnt)
    logger.info(f"Collect BP tracks :: {release_attr.clouder_week} :: Done")
    return collect_releases_tracks(release_ids)


def sp_tracks_processing(release_attr: ReleaseMeta, bp_tracks: list) -> None:
    """Collect and save SP tracks based on BP tracks."""
    clouder_week = release_attr.clouder_week
    logger.info(f"Collect spotify tracks :: {clouder_week} :: BP {len(bp_tracks)} :: Start")
    not_found = []
    sp_tracks = []
    found_cnt = 0

    for bp_track in bp_tracks:
        sp_track = get_track_by_isrc(bp_track["isrc"])
        if not sp_track:
            not_found.append(bp_track["id"])
            continue
        sp_track.pop('available_markets', None)
        sp_track['album'].pop('available_markets', None)
        sp_track["bp_id"] = bp_track["id"]
        sp_track["clouder_week"] = clouder_week
        sp_tracks.append(sp_track)

        found_cnt += 1
        if len(sp_tracks) >= 100:
            save_data_mongo_by_id(sp_tracks, "sp_tracks")
            logger.info(f"Collect spotify tracks :: {clouder_week} :: Saved {found_cnt} of {len(bp_tracks)} tracks")
            sp_tracks.clear()

    if sp_tracks:
        save_data_mongo_by_id(sp_tracks, "sp_tracks")
        logger.info(f"Collect spotify tracks :: {clouder_week} :: Saved {found_cnt} of {len(bp_tracks)} tracks")

    if not_found:
        not_found_week = [{"id": clouder_week, "not_found": not_found},]
        save_data_mongo_by_id(not_found_week, "not_found_sp_tracks")
        logger.warning(f"Spotify tracks not found :: {clouder_week} :: Count {len(not_found)}")
    release_attr.set_statistic("sp_tracks", found_cnt)
    release_attr.set_statistic("sp_not_found", len(not_found))
    logger.info(f"Collect spotify tracks :: {clouder_week} :: BP {len(bp_tracks)} :: SP {found_cnt} :: Done")


def processing_sp_playlists(release_attr: ReleaseMeta) -> None:
    """Create and fill SP playlists based on SP tracks of the week."""
    clouder_week = release_attr.clouder_week
    logger.info(f"Create spotify playlists :: {clouder_week} :: Start")

    new_tracks, old_tracks = collect_sp_week_tracks(clouder_week, release_attr.week_period[0], release_attr.week_period[1])

    new_playlist_name = release_attr.generate_sp_playlist_name("new")
    new_playlist_id = create_playlist_with_tracks(new_playlist_name, new_tracks)
    old_playlist_name = release_attr.generate_sp_playlist_name("old")
    old_playlist_id = create_playlist_with_tracks(old_playlist_name, old_tracks)

    release_attr.set_sp_playlist("new", new_playlist_id)
    release_attr.set_sp_playlist("old", old_playlist_id)
    for pl in release_attr.extra_playlists:
        pl_id = create_playlist(release_attr.generate_sp_playlist_name(pl))
        release_attr.set_sp_playlist(pl, pl_id)
    logger.info(f"Create spotify playlists :: {clouder_week} :: Done")


if __name__ == "__main__":
    env = Env()
    env.read_env()

    bp_url = env.str("BP_URL")
    bp_token = env.str("BP_TOKEN")

    release_meta = ReleaseMeta(week=34, year=2023, style_id=1)

    release_ids = bp_release_processing(release_meta, bp_url, bp_token)
    tracks_bp = bp_tracks_processing(release_meta, release_ids, bp_url, bp_token)

    sp_tracks_processing(release_meta, tracks_bp)

    save_data_mongo_by_id([release_meta.data_to_mongo()], "clouder_weeks")

    processing_sp_playlists(release_meta)
    save_data_mongo_by_id([release_meta.data_to_mongo()], "clouder_weeks")