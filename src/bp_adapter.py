import logging
import time
import random
from typing import Generator

import requests

from src.common import ReleaseMeta

logger = logging.getLogger("bp")


def bp_request(
    url: str, params: dict[str, str], bp_token: str
) -> tuple[list[dict], str, dict, bool]:
    """Make a request to the BP API and return results, next page URL, and updated params."""
    logger.info(f"Collecting page : {url} :: Start")
    if not url.startswith("https://"):
        url = f"https://{url}"
    headers = {"Authorization": f"Bearer {bp_token}"}
    r = requests.get(url, params=params, headers=headers)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.error(f"Error in request : {url} : {e}")
        return [], url, params, True
    one_page = r.json()
    next_page = one_page["next"]
    cur_page = one_page["page"]
    full_count = one_page["count"]
    logger.info(f"Collecting page : {cur_page=} : {full_count=} :: Done")
    return one_page["results"], next_page, dict(), False


def collect_releases(
    release_meta: ReleaseMeta, bp_url: str, bp_token: str, info_type: str = "releases"
) -> Generator[list[dict], None, None]:
    """Generate and yield BP releases for a given week."""
    logger.info(
        f"Collecting week : {info_type} : {release_meta} : {release_meta.week_period} :: Start"
    )
    url = f"{bp_url}/{info_type}/"
    params = {
        "genre_id": {release_meta.style_id},
        "publish_date": f"{release_meta.week_start}:{release_meta.week_end}",
        "page": 1,
        "per_page": 100,
        "order_by": "-publish_date",
    }
    while url:
        releases, url, params, _ = bp_request(url, params, bp_token)
        yield releases

    logger.info(f"Collecting week : {info_type} : {release_meta} :: Done")


def handle_one_release(
    release_id: int, bp_url: str, bp_token: str
) -> Generator[list[dict], None, None]:
    """Generate and yield tracks for a specific BP release."""
    logger.info(f"Handle release :: {release_id} :: Start")
    url = f"{bp_url}/releases/{release_id}/tracks/"
    params = {
        "page": 1,
        "per_page": 100,
    }
    while url:
        err_count = 0
        err = True
        tracks = []
        while err_count < 3 and err:
            tracks, url, params, err = bp_request(url, params, bp_token)
            err_count += 1
            time.sleep(random.randint(1, 5))
        yield tracks

    logger.info(f"Handle release :: {release_id} :: Done")
