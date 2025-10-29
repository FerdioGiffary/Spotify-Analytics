import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os
from dotenv import load_dotenv
load_dotenv()

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET"),
    redirect_uri=os.getenv("REDIRECT_URI"),
    scope="user-top-read user-library-read",
    cache_path=".spotify_token_cache"
))
print(sp.current_user()['display_name'])
