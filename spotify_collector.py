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


