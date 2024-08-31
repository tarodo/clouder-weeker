import logging
from pymongo import MongoClient, UpdateOne

from src.db_conf import get_mongo_conn

logger = logging.getLogger("clouder")

def save_bp_releases(releases, db: MongoClient = None):
    logger.info(f"Save raw releases : count = {len(releases)} :: Start")
    if db is None:
        db = get_mongo_conn()
    success = 0
    for release in releases:
        db["bp_releases"].replace_one({"id": release["id"]}, release, upsert=True)
        success += 1
    errors = len(releases) - success
    logger.info(f"Save raw releases : count = {success} : errors count = {errors} :: Done")


def save_bp_page_releases(releases, db: MongoClient = None):
    logger.info(f"Save raw releases : count = {len(releases)} :: Start")
    if db is None:
        db = get_mongo_conn()
    operations = []
    for release in releases:
        operations.append(
            UpdateOne({"id": release["id"]}, {"$set": release}, upsert=True)
        )
    if operations:
        result = db["bp_releases"].bulk_write(operations)
        print(result)
        success = result.matched_count + result.upserted_count
        errors = len(releases) - success
        logger.info(f"Save raw releases : count = {success} : errors count = {errors} :: Done")
