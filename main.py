import logging
from sqlalchemy.testing.plugin.plugin_base import logging

from src.bp_adapter import handle_one_release, collect_releases
from src.common import ReleaseMeta
from src.logging_config import setup_logging
from environs import Env

from src.mongo_adapter import collect_bp_releases, save_data_mongo

setup_logging()


logger = logging.getLogger("main")

if __name__ == "__main__":
    env = Env()
    env.read_env()
    bp_url = env.str("BP_URL")
    bp_token = env.str("BP_TOKEN")
    release_attr = ReleaseMeta(week=21, year=2023, style_id=1)
    for releases in collect_releases(release_attr, bp_url, bp_token):
        save_data_mongo(releases, "bp_releases")
    release_ids = collect_bp_releases(release_attr.week_start.isoformat(), release_attr.week_end.isoformat())
    for idx, release_id in enumerate(release_ids):
        logger.info(f"Handle release : {idx}/{len(release_ids)} :: Start")
        for tracks in handle_one_release(release_id, bp_url, bp_token):
            save_data_mongo(tracks, "bp_tracks")
        logger.info(f"Handle release : {idx}/{len(release_ids)} :: Done")

