# Create a function to read the current track in the spotify
from spotipy import SpotifyOAuth, Spotify
from environs import Env

env = Env()
env.read_env()

def create_sp():
    scope = "user-read-playback-state user-modify-playback-state"
    return Spotify(
        auth_manager=SpotifyOAuth(scope=scope, open_browser=False, show_dialog=True)
    )

def read_current_track():
    sp = create_sp()
    current_track = sp.current_playback()
    print(current_track)


read_current_track()