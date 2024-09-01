import logging
from typing import Any

from pymongo import MongoClient, UpdateOne

from src.db_conf import get_mongo_conn

logger = logging.getLogger("mongo")


def save_data_mongo_by_id(data, collection_name: str, db: MongoClient = None) -> None:
    """Save data to MongoDB by id in collection"""
    logger.info(f"Save data : {collection_name} : count = {len(data)} :: Start")
    close_connection = False
    if db is None:
        db = get_mongo_conn()
        close_connection = True
    operations = []
    for item in data:
        operations.append(UpdateOne({"id": item["id"]}, {"$set": item}, upsert=True))
    if not operations:
        logger.info(f"Save data : {collection_name} : count = 0 :: Done")
        return
    try:
        result = db[collection_name].bulk_write(operations)
        matched = result.matched_count
        upserted = result.upserted_count
        errors = len(data) - matched - upserted
        logger.info(
            f"Save data : {collection_name} : {matched=} : {upserted=} : {errors=} :: Done"
        )
    finally:
        if close_connection:
            db.client.close()


def collect_bp_releases(clouder_week: str, db: MongoClient = None) -> list:
    """Collect release ids from MongoDB"""
    close_connection = False
    if db is None:
        db = get_mongo_conn()
        close_connection = True
    try:
        result = list(
            db.bp_releases.find(
                {"clouder_week": clouder_week},
                {"_id": 0, "id": 1},
            )
        )
        return [item["id"] for item in result]
    finally:
        if close_connection:
            db.client.close()


def collect_releases_tracks(
    clouder_week: str, genre_id: int, db: MongoClient = None
) -> tuple[list[dict], list[dict]]:
    """Collect tracks from MongoDB"""
    close_connection = False
    if db is None:
        db = get_mongo_conn()
        close_connection = True
    try:
        actual = list(
            db.bp_tracks.find(
                {"clouder_week": clouder_week, "genre.id": genre_id},
                {"_id": 0, "id": 1, "isrc": 1},
            )
        )
        not_actual = list(
            db.bp_tracks.find(
                {"clouder_week": clouder_week, "genre.id": {"$ne": genre_id}},
                {"_id": 0, "id": 1, "isrc": 1},
            )
        )
        return actual, not_actual
    finally:
        if close_connection:
            db.client.close()


def collect_sp_week_tracks(
    clouder_week: str, week_start: str, db: MongoClient = None
) -> tuple[list[Any], list[Any], list[Any]]:
    """Collect new week tracks from MongoDB"""
    close_connection = False
    if db is None:
        db = get_mongo_conn()
        close_connection = True
    try:
        new_tracks = list(
            db.sp_tracks.find(
                {
                    "clouder_week": clouder_week,
                    "album.release_date": {"$gte": week_start},
                    "clouder_genre": True,
                },
                {"_id": 0, "uri": 1},
            )
        )
        new_tracks = [item["uri"] for item in new_tracks]
        old_tracks = list(
            db.sp_tracks.find(
                {
                    "clouder_week": clouder_week,
                    "album.release_date": {"$lt": week_start},
                    "clouder_genre": True,
                },
                {"_id": 0, "uri": 1},
            )
        )
        old_tracks = [item["uri"] for item in old_tracks]
        not_genre_tracks = list(
            db.sp_tracks.find(
                {"clouder_week": clouder_week, "clouder_genre": False},
                {"_id": 0, "uri": 1},
            )
        )
        not_genre_tracks = [item["uri"] for item in not_genre_tracks]
        return new_tracks, old_tracks, not_genre_tracks
    finally:
        if close_connection:
            db.client.close()
