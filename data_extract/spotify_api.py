import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from config_manager import ConfigManager

class SpotifyAPI:
    
    def __init__(self, config_path = '../config.json') -> None:
        self.config_manager = ConfigManager(config_path)
        client_id = self.config_manager.SPOTIFY_CLIENT_ID
        client_secret = self.config_manager.SPOTIFY_CLIENT_SECRET
        self.auth_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
        self.sp = spotipy.Spotify(auth_manager=self.auth_manager)

    # Function to fetch song information by track name and artist
    def get_track_info(self, song_name, artist_name):
        # Search for the song on Spotify
        results = self.sp.search(q=f'track:{song_name} artist:{artist_name}', type='track', limit=1)
        
        if results['tracks']['items']:
            track = results['tracks']['items'][0]
            song_info = {
                'name': track['name'],
                'artist': track['artists'][0]['name'],
                'album': track['album']['name'],
                'release_date': track['album']['release_date'],
                'duration_ms': track['duration_ms'],
                'popularity': track['popularity'],
                'genres': self.sp.artist(track['artists'][0]['id'])['genres']
            }
            return song_info
        else:
            return None

        