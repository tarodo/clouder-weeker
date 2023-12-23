from dbs_config import get_trans_session
from trans_models import Artist, Platform, PlatformArtist


def get_or_create_artist_bp(artist: dict, session=None):
    if not artist:
        return None

    own_session = False
    if not session:
        session = get_trans_session()
        own_session = True

    platform_id = Platform.get_id_by_short_name(session, "BP")
    platform_artist_id = artist.get("bp_id")
    if not platform_artist_id:
        raise ValueError("Artist does not have bp_id")

    artist_id = PlatformArtist.get_artist_id(session, platform_id, platform_artist_id)
    if artist_id:
        return artist_id

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

        return new_artist

    except Exception as e:
        session.rollback()
        raise e

    finally:
        if own_session:
            session.close()


get_or_create_artist_bp({"bp_id": "1", "name": "test"})
