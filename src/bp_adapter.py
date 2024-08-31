import logging

import requests

from src.common import ReleaseMeta
from src.mongo_adapter import save_bp_releases, save_bp_page_releases

logger = logging.getLogger("clouder")


def get_one_page_release(url, params, headers):
    logger.info(f"Collecting page : {url} :: Start")
    if not url.startswith("https://"):
        url = f"https://{url}"
    r = requests.get(url, params=params, headers=headers)
    r.raise_for_status()
    release_page = r.json()
    next_page = release_page["next"]
    cur_page = release_page["page"]
    full_count = release_page["count"]
    logger.info(f"Collecting page : {cur_page=} : {full_count=} :: Done")
    return release_page["results"], next_page, dict()


def collect_releases(release_meta: ReleaseMeta, bp_url: str, bp_token: str) -> bool:
    logger.info(f"Collecting week : {release_meta} : {release_meta.week_start} : {release_meta.week_end} :: Start")
    url = f"{bp_url}/"
    params = {
        "genre_id": {release_meta.style_id},
        "publish_date": f"{release_meta.week_start}:{release_meta.week_end}",
        "page": 1,
        "per_page": 100,
        "order_by": "-publish_date",
    }
    headers = {"Authorization": f"Bearer {bp_token}"}
    while url:
        releases, url, params = get_one_page_release(url, params, headers)
        save_bp_page_releases(releases)
    logger.info(f"Collecting week : {release_meta} :: Done")
    return True