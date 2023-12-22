from urllib.parse import urlparse, quote, urlunparse

from sqlalchemy import create_engine
from models import Base
from environs import Env

env = Env()
env.read_env()

user = env.str("PG_USER")
password = env.str("PG_PASS")
host = env.str("PG_HOST")
port = env.str("PG_PORT")
db = env.str("PG_DB")

url = urlparse("")
url = url._replace(
    scheme="postgresql", netloc=f"{user}:{quote(password)}@{host}:{port}/{db}"
)
engine = create_engine(urlunparse(url))

Base.metadata.create_all(bind=engine)