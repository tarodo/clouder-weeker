from functools import lru_cache

from sqlalchemy import Column, ForeignKey, Integer, String, Date, Table
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Platform(Base):
    __tablename__ = "platforms"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    short_name = Column(String, unique=True)
    artist_collection = Column(String, unique=True)
    track_collection = Column(String, unique=True)

    @classmethod
    @lru_cache(maxsize=10)
    def get_id_by_short_name(cls, session, short_name):
        platform = session.query(cls).filter_by(short_name=short_name).first()
        return platform.id if platform else None

    @classmethod
    def create_init(cls, session):
        existing_names = {platform.name for platform in session.query(cls.name).all()}
        new_platforms = [
            cls(
                name="Beatport",
                short_name="BP",
                artist_collection="bp_artists",
                track_collection="bp_tracks",
            ),
            cls(
                name="Spotify",
                short_name="SP",
                artist_collection="sp_artists",
                track_collection="sp_tracks",
            ),
        ]

        for platform in new_platforms:
            if platform.name not in existing_names:
                session.add(platform)

        try:
            session.commit()
        except Exception:
            session.rollback()
            raise


def get_init_platforms() -> list[Platform]:
    return [
        Platform(
            name="Beatport",
            short_name="BP",
            artist_collection="bp_artists",
            track_collection="bp_tracks",
        ),
        Platform(
            name="Spotify",
            short_name="SP",
            artist_collection="sp_artists",
            track_collection="sp_tracks",
        ),
    ]


class Style(Base):
    __tablename__ = "styles"
    id = Column(Integer, primary_key=True, autoincrement=False)
    name = Column(String, unique=True)

    @classmethod
    def create_init(cls, session):
        existing_names = {style.name for style in session.query(cls.name).all()}
        new_styles = [cls(id=1, name="DNB"), cls(id=90, name="TECHNO")]

        session.add_all(
            style for style in new_styles if style.name not in existing_names
        )

        try:
            session.commit()
        except Exception:
            session.rollback()
            raise


artist_track_association = Table(
    "artist_track",
    Base.metadata,
    Column("artist_id", Integer, ForeignKey("artists.id")),
    Column("track_id", Integer, ForeignKey("tracks.id")),
)


class Artist(Base):
    __tablename__ = "artists"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    tracks = relationship(
        "Track", secondary=artist_track_association, back_populates="artists"
    )


class Track(Base):
    __tablename__ = "tracks"
    id = Column(Integer, primary_key=True)
    title = Column(String)
    isrc = Column(String)
    artists = relationship(
        "Artist", secondary=artist_track_association, back_populates="tracks"
    )
    style_id = Column(Integer, ForeignKey("styles.id"), nullable=False)


class PlatformArtist(Base):
    __tablename__ = "platforms_artists"
    platform_id = Column(Integer, ForeignKey("platforms.id"), primary_key=True)
    artist_id = Column(Integer, ForeignKey("artists.id"), primary_key=True)
    platform_artist_id = Column(String, nullable=False)

    @classmethod
    @lru_cache()
    def get_artist_id(cls, session, platform_id, platform_artist_id):
        platform_artist = (
            session.query(cls)
            .filter_by(platform_id=platform_id, platform_artist_id=platform_artist_id)
            .first()
        )
        return platform_artist.id if platform_artist else None


class PlatformTrack(Base):
    __tablename__ = "platforms_tracks"
    track_id = Column(Integer, ForeignKey("tracks.id"), primary_key=True)
    platform_id = Column(Integer, ForeignKey("platforms.id"), primary_key=True)
    platform_track_id = Column(String)
    publish_date = Column(Date)
    release_date = Column(Date)
