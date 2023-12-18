from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth


def create_sp():
    scope = "playlist-modify-public"
    return Spotify(
        auth_manager=SpotifyOAuth(scope=scope, open_browser=False, show_dialog=True)
    )


def get_track_by_isrc(isrc: str, sp: Spotify = None):
    track_result = sp.search(q=f"isrc:{isrc}", type="track", limit=1)
    tracks = track_result["tracks"]["items"]
    if tracks:
        sp_track = tracks[0]
        return sp_track
    return None


def create_playlist(sp: Spotify, title: str) -> (str, str):
    user_id = sp.me()["id"]
    playlist = sp.user_playlist_create(user_id, title)
    return playlist["id"], playlist["external_urls"]["spotify"]


def add_tracks(sp: Spotify, playlist_id: str, tracks_ids: list[str]):
    pack_size = 100
    parts = [
        tracks_ids[i : i + pack_size] for i in range(0, len(tracks_ids), pack_size)
    ]
    for part in parts:
        sp.playlist_add_items(playlist_id, part)
    return True
