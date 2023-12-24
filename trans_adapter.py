from sqlalchemy.orm import Session
from dbs_config import with_session
from trans_models import Artist, Platform, PlatformArtist, PlatformTrack, Track


@with_session
def get_or_create_artist_bp(artist: dict, session: Session = None) -> int:
    if not artist:
        raise ValueError("Artist is empty")

    platform_id = Platform.get_id_by_short_name(session, "BP")
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
    platform_id = Platform.get_id_by_short_name(session, "BP")
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


if __name__ == "__main__":
    art_1 = {"name": "Artist 1", "bp_id": "1"}
    art_2 = {"name": "Artist 2", "bp_id": "2"}
    art_ids = [get_or_create_artist_bp(art_1), get_or_create_artist_bp(art_2)]
    track_1 = {"title": "Track 1", "style_id": 1, "bp_id": "1"}
    track_id = get_or_create_track_bp(track_1, art_ids)
    print(track_id)
