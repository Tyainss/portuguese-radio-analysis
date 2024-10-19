import polars as pl
import aiohttp
import asyncio
from config_manager import ConfigManager

class AsyncSpotifyAPI:
    
    def __init__(self) -> None:
        self.config_manager = ConfigManager()
        self.client_id = self.config_manager.SPOTIFY_CLIENT_ID
        self.client_secret = self.config_manager.SPOTIFY_CLIENT_SECRET
        self.token_url = 'https://accounts.spotify.com/api/token'
        self.base_url = 'https://api.spotify.com/v1'
        self.access_token = None

    async def authenticate(self):
        """Obtain the access token asynchronously using client credentials."""
        auth_headers = {
            'Authorization': f'Basic {self._encode_client_credentials()}',
        }
        data = {
            'grant_type': 'client_credentials'
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(self.token_url, headers=auth_headers, data=data) as response:
                if response.status == 200:
                    token_data = await response.json()
                    self.access_token = token_data['access_token']
                else:
                    raise Exception(f"Failed to authenticate: {response.status}")

    def _encode_client_credentials(self):
        """Helper function to encode client credentials in base64."""
        import base64
        credentials = f"{self.client_id}:{self.client_secret}"
        return base64.b64encode(credentials.encode()).decode()

    async def _make_request(self, endpoint, params=None):
        """Helper function to make asynchronous GET requests to Spotify API."""
        headers = {
            'Authorization': f'Bearer {self.access_token}',
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.base_url}/{endpoint}", headers=headers, params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    raise Exception(f"API request failed with status code {response.status}: {await response.text()}")

    async def get_track_info(self, track_name, artist_name):
        """Asynchronous function to fetch track information by track name and artist."""
        # First, ensure that we're authenticated
        if not self.access_token:
            await self.authenticate()

        # Search for the track
        search_params = {
            'q': f'track:{track_name} artist:{artist_name}',
            'type': 'track',
            'limit': 1
        }
        search_results = await self._make_request('search', search_params)
        if search_results['tracks']['items']:
            track = search_results['tracks']['items'][0]
            artist_id = track['artists'][0]['id']
            
            # Fetch genres for the artist
            artist_info = await self._make_request(f'artists/{artist_id}')
            genres = artist_info.get('genres', [])
            
            # Build the track info dictionary
            track_info = {
                'name': track['name'],
                'artist': track['artists'][0]['name'],
                'album': track['album']['name'],
                'release_date': track['album']['release_date'],
                'duration_ms': track['duration_ms'],
                'popularity': track['popularity'],
                'genres': genres
            }
            return track_info
        else:
            return None
        
    async def fetch_all_track_info(self, df):
        """Fetch Spotify info for all rows in the dataframe."""
        tasks = []
        for row in df.iter_rows(named=True):
            track_name = row[self.config_manager.TRACK_TITLE_COLUMN]
            artist_name = row[self.config_manager.TRACK_ARTIST_COLUMN]
            tasks.append(self.get_track_info(track_name, artist_name))
        return await asyncio.gather(*tasks)

    async def process_data(self, df):
        """Fetch track info and return a dataframe with Spotify data joined."""
        # Fetch track info for all rows
        spotify_data = await self.fetch_all_track_info(df)

        # Filter out None values and create a new dataframe with the Spotify data
        spotify_data_filtered = [track for track in spotify_data if track]
        spotify_df = pl.DataFrame(spotify_data_filtered)

        # Merge with the original dataframe
        return spotify_df


if __name__ ==  "__main__":
    # Create an instance of the AsyncSpotifyAPI
    spotify_api = AsyncSpotifyAPI()

    # Run the asynchronous function using asyncio
    async def fetch_track():
        track_info = await spotify_api.get_track_info('Shape of You', 'Ed Sheeran')
        print(track_info)

    async def process(df):
        spotify_df = await spotify_api.process_data(df)
        print(spotify_df)

    df = pl.DataFrame({
    'TRACK_TITLE': ['Houdini', 'Please Please Please'],
    'TRACK_ARTIST': ['Dua Lipa', 'Sabrina Carpenter']
    })

    # Run the event loop
    # asyncio.run(fetch_track())

    asyncio.run(process(df))

    # Run the event loop
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(fetch_track())