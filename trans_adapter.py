from sqlalchemy.orm import Session

from dbs_config import get_trans_session, with_session
from trans_models import Artist, Platform, PlatformArtist, PlatformTrack, Track


def get_artist_by_id(artist_id: int, session) -> Artist:
    return session.query(Artist).filter_by(id=artist_id).first()


def get_artist_id(session, platform_id, platform_artist_id):
    platform_artist: PlatformArtist | None = (
        session.query(PlatformArtist)
        .filter_by(platform_id=platform_id, platform_artist_id=platform_artist_id)
        .first()
    )
    return platform_artist.artist if platform_artist else None


@with_session
def get_or_create_artist_bp(artist: dict, session=None) -> int:
    if not artist:
        raise ValueError("Artist is empty")

    platform_id = Platform.get_id_by_short_name(session, "BP")
    platform_artist_id = artist.get("bp_id")
    if not platform_artist_id:
        raise ValueError("Artist does not have bp_id")

    old_artist = PlatformArtist.get_artist_id(session, platform_id, platform_artist_id)
    if old_artist:
        return old_artist.id

    artist_name = artist["name"]
    new_artist = Artist(name=artist_name)
    try:
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

    except Exception:
        session.rollback()
        raise


def get_track(session: Session, platform_id: str, platform_track_id: str) -> Track:
    platform_track: PlatformTrack | None = (
        session.query(PlatformTrack)
        .filter_by(platform_id=platform_id, platform_track_id=platform_track_id)
        .first()
    )
    return platform_track.track if platform_track else None


@with_session
def get_or_create_track_bp(track: dict, artists_ids, session=None) -> int:
    try:
        artists = [get_artist_by_id(artist_id, session) for artist_id in artists_ids]
        platform_id = Platform.get_id_by_short_name(session, "BP")
        platform_track_id = track.get("bp_id")
        if not platform_track_id:
            raise ValueError("Track does not have bp_id")
        old_track: Track = get_track(session, platform_id, platform_track_id)
        if old_track:
            return old_track.id
    except Exception:
        raise


if __name__ == "__main__":
    artist = get_or_create_artist_bp({"bp_id": "sdffds1", "name": "DJ Bublik 1"})
    print(artist)