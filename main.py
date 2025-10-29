
from dotenv import load_dotenv
import os
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas_gbq as pg
from google.oauth2 import service_account

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")

auth_manager = SpotifyOAuth(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    redirect_uri=REDIRECT_URI,
    scope="user-top-read user-read-recently-played"
)

sp = spotipy.Spotify(auth_manager=auth_manager)

top_tracks = sp.current_user_top_tracks(limit=10, time_range='short_term')
top_items = top_tracks.get("items", [])
top_ids = [t["id"] for t in top_items]

recent = sp.current_user_recently_played(limit=50)
recent_items = recent.get("items", [])

recent_count = {}
for play in recent_items:
    track = play.get("track") or {}
    tid = track.get("id")
    if tid:
        recent_count[tid] = recent_count.get(tid, 0) + 1

rows = []
for t in top_items:
    tid = t["id"]
    album = t.get("album", {})
    artist = t["artists"][0]
    rows.append({
        "track_id": tid,
        "track_name": t.get("name"),
        "artist_name": artist.get("name"),
        "album_name": album.get("name"),
        "album_release_date": album.get("release_date"),
        "popularity": t.get("popularity"),
        "duration_ms": t.get("duration_ms"),
        "explicit": t.get("explicit"),
        "recent_play_count": recent_count.get(tid, 0),  # ---> number of times in last 50 plays, bukan total played
        "track_url": t.get("external_urls", {}).get("spotify")
    })

df = pd.DataFrame(rows)
df["duration_min"] = df["duration_ms"] / 60000.0

PROJECT_ID = 'project-jcdsbsdam29-0005'
KEY_PATH = 'project-jcdsbsdam29-0005-f517dc7082bd.json'
TABLE = 'spotify_analytics.capstone_2_test'
scopes = ['https://www.googleapis.com/auth/bigquery']
credentials = service_account.Credentials.from_service_account_file(KEY_PATH, scopes=scopes)

pg.to_gbq(
    dataframe = df,
    destination_table=TABLE,
    project_id=PROJECT_ID,
    credentials=credentials,
    if_exists='replace'
)