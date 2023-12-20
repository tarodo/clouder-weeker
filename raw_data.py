import json
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
    db["bp_releases"].create_index([("new_release_date", -1)], unique=True)


def load_bp_releases(releases: Iterable, db: MongoClient = None):
    if not db:
        db = get_db()
    db["bp_releases"].insert_many(releases)

# release_file = "data/DNB/2023/10/week_raw/page_1.json"
# releases = json.load(open(release_file, "r"))
#
# bp_releases = db["results"]
# db.bp_releases.insert_many(releases["results"])
