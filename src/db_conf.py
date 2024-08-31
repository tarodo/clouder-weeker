import os
from urllib.parse import urlparse, quote, urlunparse

from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError


def get_mongo_conn():
    try:
        user = os.getenv("MONGO_USER")
        password = os.getenv("MONGO_PASS")
        host = os.getenv("MONGO_HOST")
        port = os.getenv("MONGO_PORT")
        db = os.getenv("MONGO_DB")
    except Exception as e:
        raise KeyError(f"Environment variables for MongoDB are not set correctly. :: {e}")

    try:
        url = urlparse("")
        url = url._replace(
            scheme="mongodb", netloc=f"{user}:{quote(password)}@{host}:{port}"
        )
        client = MongoClient(str(urlunparse(url)))
        return client[db]
    except ServerSelectionTimeoutError as e:
        raise KeyError(f"Failed to connect to MongoDB database. :: {e}")