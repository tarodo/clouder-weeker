from urllib.parse import quote, urlparse, urlunparse

from environs import Env, EnvError
from pymongo import MongoClient, errors
from pymongo.database import Database


def get_mongo_conn() -> Database:
    """Connects to MongoDB and returns the database object."""
    try:
        env = Env()
        env.read_env()

        user = env.str("MONGO_USER")
        password = env.str("MONGO_PASS")
        host = env.str("MONGO_HOST")
        port = env.str("MONGO_PORT")
        db_name = env.str("MONGO_DB")
    except EnvError as e:
        raise KeyError(
            f"Environment variables for MongoDB are not set correctly. :: {e}"
        )

    try:
        url = urlparse("")
        url = url._replace(
            scheme="mongodb", netloc=f"{user}:{quote(password)}@{host}:{port}"
        )
        client = MongoClient(str(urlunparse(url)), serverSelectionTimeoutMS=5000)
        client.admin.command("ping")  # Test the connection
        return client[db_name]
    except (errors.ServerSelectionTimeoutError, errors.PyMongoError) as e:
        raise Exception(f"Failed to connect to MongoDB database. :: {e}")


def init_mongo_db() -> None:
    """Initializes MongoDB collections with required indexes."""
    db = get_mongo_conn()
    db["bp_releases"].create_index([("id", -1)], unique=True)
    db["bp_releases"].create_index([("publish_date", -1)])

    db["bp_tracks"].create_index([("id", -1)], unique=True)
    db["bp_tracks"].create_index([("new_release_date", -1)])
    db["bp_tracks"].create_index([("publish_date", -1)])
    db["bp_tracks"].create_index([("isrc", 1)])

    db["sp_tracks"].create_index([("id", -1)], unique=True)
    db["sp_tracks"].create_index([("album.release_date", -1)])


if __name__ == "__main__":
    init_mongo_db()
