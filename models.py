from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Platform(Base):
    __tablename__ = "platforms"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    short_name = Column(String)
    artist_collection = Column(String)
    track_collection = Column(String)


class Artist(Base):
    __tablename__ = "artists"
    id = Column(Integer, primary_key=True)
    name = Column(String)


class Track(Base):
    __tablename__ = "tracks"
    id = Column(Integer, primary_key=True)
    title = Column(String)
    isrc = Column(String)
    artist_id = Column(Integer, ForeignKey('artists.id'))


class PlatformArtist(Base):
    __tablename__ = "platforms_artists"
    platform_id = Column(Integer, ForeignKey('platforms.id'), primary_key=True)
    artist_id = Column(Integer, ForeignKey('artists.id'), primary_key=True)
    platform_artist_id = Column(String)


class PlatformTrack(Base):
    __tablename__ = "platforms_tracks"
    track_id = Column(Integer, ForeignKey('tracks.id'), primary_key=True)
    platform_id = Column(Integer, ForeignKey('platforms.id'), primary_key=True)
    platform_track_id = Column(String)

