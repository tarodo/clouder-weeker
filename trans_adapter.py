from functools import lru_cache

from sqlalchemy.orm import Session
from dbs_config import with_session
from trans_models import Artist, Platform, PlatformArtist, PlatformTrack, Track


@lru_cache(maxsize=10)
@with_session
def get_platform_id(short_name, session: Session = None) -> int:
    platform = session.query(Platform).filter_by(short_name=short_name).first()
    return platform.id if platform else None


@with_session
def get_or_create_artist_bp(artist: dict, session: Session = None) -> int:
    if not artist:
        raise ValueError("Artist is empty")

    platform_id = get_platform_id("BP")
    platform_artist_id = artist.get("bp_id")
    if not platform_artist_id:
        raise ValueError("Artist does not have bp_id")

    old_artist = session.query(PlatformArtist).filter_by(platform_id=platform_id,
                                                         platform_artist_id=platform_artist_id).first()
    if old_artist:
        return old_artist.artist.id

    new_artist = Artist(name=artist["name"])
    session.add(new_artist)
    session.flush()

    platform_artist = PlatformArtist(
        platform_id=platform_id,
        platform_artist_id=platform_artist_id,
        artist_id=new_artist.id,
    )
    session.add(platform_artist)
    session.commit()

    return new_artist.id


@with_session
def get_or_create_track_bp(track: dict, artists_ids, session=None) -> int:
    artists = [session.query(Artist).filter_by(id=artist_id).first() for artist_id in artists_ids]
    platform_id = get_platform_id("BP")
    platform_track_id = track.get("bp_id")
    if not platform_track_id:
        raise ValueError("Track does not have bp_id")

    old_track = session.query(PlatformTrack).filter_by(platform_id=platform_id,
                                                       platform_track_id=platform_track_id).first()
    if old_track:
        return old_track.track.id

    new_track = Track(title=track["title"], artists=artists, style_id=track["style_id"])
    session.add(new_track)
    session.flush()

    platform_track = PlatformTrack(
        platform_id=platform_id,
        platform_track_id=platform_track_id,
        track_id=new_track.id,
    )
    session.add(platform_track)
    session.commit()

    return new_track.id


@with_session
def connect_track_sp_to_bp(track_bp_id, track_sp_id, session=None):
    bp_platform_id = get_platform_id("BP")
    sp_platform_id = get_platform_id("SP")
    old_track_bp = session.query(PlatformTrack).filter_by(platform_id=bp_platform_id,
                                                          platform_track_id=track_bp_id).first()
    if not old_track_bp:
        raise ValueError("Track does not exist")

    old_track_sp = session.query(PlatformTrack).filter_by(platform_id=sp_platform_id,
                                                          platform_track_id=track_sp_id).first()
    if old_track_sp:
        if old_track_sp.track.id != old_track_bp.track.id:
            raise ValueError("Track is already connected to another BP track")
        return old_track_sp.track.id

    new_track_sp = PlatformTrack(
        platform_id=sp_platform_id,
        platform_track_id=track_sp_id,
        track_id=old_track_bp.track.id,
    )
    session.add(new_track_sp)
    session.commit()

    return new_track_sp.track.id


if __name__ == "__main__":
    art_1 = {"name": "Artist 1", "bp_id": "1"}
    art_2 = {"name": "Artist 2", "bp_id": "2"}
    art_ids = [get_or_create_artist_bp(art_1), get_or_create_artist_bp(art_2)]
    track_1 = {"title": "Track 1", "style_id": 1, "bp_id": "bp1"}
    track_id = get_or_create_track_bp(track_1, art_ids)
    print(track_id)
    track_id = connect_track_sp_to_bp("bp1", "sp1")
    print(track_id)
