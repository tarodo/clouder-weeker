import json
import logging
import os
from pathlib import Path

import requests
from environs import Env

import week_dates

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("weeker")

DATA_DIR = "data"


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
    url = "https://api.beatport.com/v4/catalog/releases/"
    params = {
        "genre_id": {genre_id},
        "publish_date": f"{start_date}:{end_date}",
        "page": 1,
        "per_page": 100,
        "order_by": "-publish_date",
    }
    headers = {"Authorization": f"Bearer {bp_token}"}
    data_dir = data_dir / "week_raw"
    os.makedirs(data_dir, exist_ok=True)
    while url:
        url, params = save_one_page(url, params, headers, data_dir)


def handle_week(year: int, week_number: int, style_id: int, bp_token: str):
    week_number = str(week_number).zfill(2)
    style_name = BP_STYLES.get(style_id)
    start, end = week_dates.get_start_end_dates(year, week_number)
    data_path = Path(DATA_DIR) / style_name / f"{week_number}"
    os.makedirs(data_path, exist_ok=True)

    logger.info(
        f"Start :: {style_name} :: Year : {year} Week : {week_number} :: {start} : {end}"
    )
    collect_week(start, end, style_id, bp_token, data_path)


if __name__ == "__main__":
    env = Env()
    env.read_env()
    bp_token = env.str("BP_TOKEN")

    handle_week(2023, 10, 1, bp_token)
