import logging
from typing import Iterable

from environs import Env
from pymongo import MongoClient

from dbs_config import get_raw_db

env = Env()
env.read_env()
logger = logging.getLogger(__name__)


def load_bp_releases(releases: Iterable, db: MongoClient = None):
    if db is None:
        db = get_raw_db()
    for release in releases:
        db["bp_releases"].replace_one({"id": release["id"]}, release, upsert=True)


def collect_bp_releases(week_start: str, week_end: str, db: MongoClient = None):
    if db is None:
        db = get_raw_db()
    return db.bp_releases.find(
        {"publish_date": {"$gte": week_start, "$lte": week_end}}, {"_id": 0, "id": 1}
    )


def load_bp_track(release_track: dict, db: MongoClient = None):
    logger.info(f"Load raw track : {release_track['id']} :: Start")
    if db is None:
        db = get_raw_db()
    db["bp_tracks"].replace_one(
        {"id": release_track["id"]}, release_track, upsert=True
    )
    logger.info(f"Load raw track : {release_track['id']} :: Done")


def collect_bp_tracks(week_start: str, week_end: str, db: MongoClient = None):
    if db is None:
        db = get_raw_db()
    return db.bp_tracks.find(
        {"publish_date": {"$gte": week_start, "$lte": week_end}},
        {"_id": 0, "isrc": 1, "id": 1},
    )


def load_sp_track(sp_track: dict, db: MongoClient = None):
    if db is None:
        db = get_raw_db()
    db["sp_tracks"].replace_one({"id": sp_track["id"]}, sp_track, upsert=True)


def collect_week_sp_tracks(year: int, week: int, db: MongoClient = None):
    if db is None:
        db = get_raw_db()
    return db.sp_tracks.find(
        {
            "bp_year": year,
            "bp_week": week,
            "album.release_date": {
                "$gte": str(year),
            },
        },
        {"_id": 0, "id": 1},
    )
