from typing import Iterable
from urllib.parse import urlparse, quote, urlunparse

from pymongo import MongoClient
from environs import Env

env = Env()
env.read_env()


def get_db():
    user = env.str("MONGO_USER")
    password = env.str("MONGO_PASS")
    host = env.str("MONGO_HOST")
    port = env.str("MONGO_PORT")
    db = env.str("MONGO_DB")

    url = urlparse('')
    url = url._replace(scheme='mongodb', netloc=f'{user}:{quote(password)}@{host}:{port}')

    client = MongoClient(urlunparse(url))
    return client[db]


def init_db(db: MongoClient = None):
    if not db:
        db = get_db()
    db["bp_releases"].create_index([("id", -1)], unique=True)
    db["bp_releases"].create_index([("new_release_date", -1)])

    db["bp_release_tracks"].create_index([("id", -1)], unique=True)
    db["bp_release_tracks"].create_index([("new_release_date", -1)])


def load_bp_releases(releases: Iterable, db: MongoClient = None):
    if not db:
        db = get_db()
    db["bp_releases"].insert_many(releases)


def collect_bp_releases(week_start: str, week_end: str, db: MongoClient = None):
    if not db:
        db = get_db()
    return db.bp_releases.find({
        "publish_date": {
            "$gte": week_start,
            "$lte": week_end
        }}, {"_id": 0, "id": 1}
    )


def load_bp_release_tracks(release_tracks: Iterable, db: MongoClient = None):
    if not db:
        db = get_db()
    print(release_tracks)
    # db["bp_release_tracks"].insert_many(release_tracks)


if __name__ == "__main__":
    init_db()
