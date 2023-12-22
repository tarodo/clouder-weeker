from urllib.parse import quote, urlparse, urlunparse

from environs import Env
from sqlalchemy import Column, Integer, String, create_engine, text
from sqlalchemy.orm import Session, declarative_base

env = Env()
env.read_env()

Base = declarative_base()


class BPArtist(Base):
    __tablename__ = "artists"
    __table_args__ = {"schema": "beatport"}
    id = Column(Integer, primary_key=True)
    name = Column(String)
    slug = Column(String)
    url = Column(String)


user = env.str("PG_USER")
password = env.str("PG_PASS")
host = env.str("PG_HOST")
port = env.str("PG_PORT")
db = env.str("PG_DB")

url = urlparse("")
url = url._replace(
    scheme="postgresql", netloc=f"{user}:{quote(password)}@{host}:{port}", path=f"/{db}"
)

engine = create_engine(urlunparse(url))

with Session(engine) as session:
    print("Creating schema... 88")
    session.execute(text("CREATE SCHEMA IF NOT EXISTS beatport"))
    session.commit()

Base.metadata.create_all(bind=engine)
