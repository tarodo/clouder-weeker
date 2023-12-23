from sqlalchemy import Column, ForeignKey, Integer, String, Date
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Platform(Base):
    __tablename__ = "platforms"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    short_name = Column(String)
    artist_collection = Column(String)
    track_collection = Column(String)


def get_init_platforms() -> list[Platform]:
    return [
        Platform(name="Beatport", short_name="BP", artist_collection="bp_artists", track_collection="bp_tracks"),
        Platform(name="Spotify", short_name="SP", artist_collection="sp_artists", track_collection="sp_tracks"),
    ]


class Style(Base):
    __tablename__ = "styles"
    id = Column(Integer, primary_key=True, autoincrement=False)
    name = Column(String)


def get_init_styles() -> list[Style]:
    return [
        Style(id=1, name="DNB"),
        Style(id=90, name="TECHNO"),
    ]


class Artist(Base):
    __tablename__ = "artists"
    id = Column(Integer, primary_key=True)
    name = Column(String)


class Track(Base):
    __tablename__ = "tracks"
    id = Column(Integer, primary_key=True)
    title = Column(String)
    isrc = Column(String)
    artist_id = Column(Integer, ForeignKey("artists.id"), nullable=False)
    style_id = Column(Integer, ForeignKey("styles.id"), nullable=False)


class PlatformArtist(Base):
    __tablename__ = "platforms_artists"
    platform_id = Column(Integer, ForeignKey("platforms.id"), primary_key=True)
    artist_id = Column(Integer, ForeignKey("artists.id"), primary_key=True)
    platform_artist_id = Column(String)


class PlatformTrack(Base):
    __tablename__ = "platforms_tracks"
    track_id = Column(Integer, ForeignKey("tracks.id"), primary_key=True)
    platform_id = Column(Integer, ForeignKey("platforms.id"), primary_key=True)
    platform_track_id = Column(String)
    publish_date = Column(Date)
    release_date = Column(Date)
