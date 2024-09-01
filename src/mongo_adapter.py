import logging
from pymongo import MongoClient, UpdateOne

from src.db_conf import get_mongo_conn

logger = logging.getLogger("mongo")


def save_data_mongo(data, collection_name: str, db: MongoClient = None):
    """Save data to MongoDB by id in collection"""
    logger.info(f"Save data : {collection_name} : count = {len(data)} :: Start")
    close_connection = False
    if db is None:
        db = get_mongo_conn()
        close_connection = True
    operations = []
    for item in data:
        operations.append(
            UpdateOne({"id": item["id"]}, {"$set": item}, upsert=True)
        )
    if not operations:
        logger.info(f"Save data : {collection_name} : count = 0 :: Done")
        return
    try:
        result = db[collection_name].bulk_write(operations)
        matched = result.matched_count
        upserted = result.upserted_count
        errors = len(data) - matched - upserted
        logger.info(f"Save data : {collection_name} : {matched=} : {upserted=} : {errors=} :: Done")
    finally:
        if close_connection:
            db.client.close()


def collect_bp_releases(week_start: str, week_end: str, db: MongoClient = None) -> list:
    """Collect release ids from MongoDB"""
    close_connection = False
    if db is None:
        db = get_mongo_conn()
        close_connection = True
    try:
        result = list(
            db.bp_releases.find(
                {"publish_date": {"$gte": week_start, "$lte": week_end}},
                {"_id": 0, "id": 1}
            )
        )
        return [item["id"] for item in result]
    finally:
        if close_connection:
            db.client.close()
