from functools import wraps
from inspect import signature
from urllib.parse import quote, urlparse, urlunparse

from environs import Env, EnvError
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from trans_models import Base, Platform, Style

env = Env()
env.read_env()


def get_engine():
    try:
        user = env.str("PG_USER")
        password = env.str("PG_PASS")
        host = env.str("PG_HOST")
        port = env.str("PG_PORT")
        db = env.str("PG_DB")
    except EnvError:
        print("Environment variables for PostgreSQL are not set correctly.")
        raise

    url = urlparse("")
    url = url._replace(
        scheme="postgresql", netloc=f"{user}:{quote(password)}@{host}:{port}/{db}"
    )
    return create_engine(str(urlunparse(url)))


def get_trans_session(engine=None):
    if engine is None:
        engine = get_engine()
    return sessionmaker(bind=engine)()


def get_raw_db():
    try:
        user = env.str("MONGO_USER")
        password = env.str("MONGO_PASS")
        host = env.str("MONGO_HOST")
        port = env.str("MONGO_PORT")
        db = env.str("MONGO_DB")
    except EnvError:
        print("Environment variables for MongoDB are not set correctly.")
        raise

    try:
        url = urlparse("")
        url = url._replace(
            scheme="mongodb", netloc=f"{user}:{quote(password)}@{host}:{port}"
        )
        client = MongoClient(str(urlunparse(url)))
        return client[db]
    except ServerSelectionTimeoutError:
        print("Failed to connect to MongoDB database.")
        raise


def reg_init_data(engine):
    with get_trans_session(engine) as session:
        Style.create_init(session)
        Platform.create_init(session)


def init_trans_db(engine=None):
    if engine is None:
        engine = get_engine()
    try:
        Base.metadata.create_all(bind=engine)
        reg_init_data(engine)
    except OperationalError:
        print("PostgreSQL is not available.")
        raise


def init_raw_db(db: MongoClient = None):
    if db is None:
        db = get_raw_db()
    db["bp_releases"].create_index([("id", -1)], unique=True)
    db["bp_releases"].create_index([("publish_date", -1)])

    db["bp_tracks"].create_index([("id", -1)], unique=True)
    db["bp_tracks"].create_index([("new_release_date", -1)])
    db["bp_tracks"].create_index([("publish_date", -1)])
    db["bp_tracks"].create_index([("isrc", 1)])

    db["sp_tracks"].create_index([("id", -1)], unique=True)
    db["sp_tracks"].create_index([("bp_year", -1), ("bp_week", -1)])
    db["sp_tracks"].create_index([("external_ids.isrc", 1)])
    db["sp_tracks"].create_index([("album.release_date", -1)])


def init_databases():
    try:
        init_raw_db()
        init_trans_db()
    except (EnvError, ServerSelectionTimeoutError, OperationalError):
        print("Failed to initialize databases.")


def with_session(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        func_sig = signature(func)
        accepts_session = "session" in func_sig.parameters

        session_provided = "session" in kwargs
        if accepts_session and not session_provided:
            print(f"Session is not provided for {func.__name__}")
            kwargs["session"] = get_trans_session()
            session_created = True
        else:
            session_created = False

        try:
            result = func(*args, **kwargs)
        finally:
            if session_created:
                kwargs["session"].close()

        return result

    return wrapper


if __name__ == "__main__":
    init_databases()
