import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

def load_json(path='config.json'):
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        print("Error: File not found")

config = load_json()

client_id = config['CLIENT_ID']
client_secret = config['API_KEY']

# Set up the client credentials manager
auth_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(auth_manager=auth_manager)

# Function to fetch song information by track name and artist
def get_song_info(song_name, artist_name):
    # Search for the song on Spotify
    results = sp.search(q=f'track:{song_name} artist:{artist_name}', type='track', limit=1)
    
    if results['tracks']['items']:
        track = results['tracks']['items'][0]
        song_info = {
            'name': track['name'],
            'artist': track['artists'][0]['name'],
            'album': track['album']['name'],
            'release_date': track['album']['release_date'],
            'popularity': track['popularity'],
            'genres': sp.artist(track['artists'][0]['id'])['genres']
        }
        return song_info
    else:
        return None

# Example usage
song_name = "Alô"
artist_name = "Dillaz"
song_info = get_song_info(song_name, artist_name)

if song_info:
    print(f"Song Info: {song_info}")
else:
    print("Song not found.")