import polars as pl
import aiohttp
import asyncio
from tqdm.asyncio import tqdm_asyncio

from config_manager import ConfigManager
from helper import Helper

class AsyncSpotifyAPI:
    
    def __init__(self) -> None:
        self.config_manager = ConfigManager()
        self.helper = Helper()
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

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=90)) as session:
            for attempt in range(5):
                try:
                    async with session.post(self.token_url, headers=auth_headers, data=data) as response:
                        if response.status == 200:
                            token_data = await response.json()
                            self.access_token = token_data['access_token']
                            # print("Authenticated successfully")
                            return
                        elif response.status == 502:
                            wait_time = 2 ** attempt
                            print(f"Received 502 error. Retrying after {wait_time} seconds.")
                            await asyncio.sleep(wait_time)
                        else:
                            print(f"Failed to authenticate: Status {response.status}")
                            await response.text()
                except aiohttp.ClientConnectorError as e:
                    print(f"Connection error during authentication attempt {attempt + 1}: {e}")
                    await asyncio.sleep(2 ** attempt)
                except Exception as e:
                    print(f"Unexpected error during authentication: {e}")
            raise Exception("Max retries exceeded during authentication")

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
            previous_retry_after = None
            last_printed_message = None  # Track the last printed message
            while True:  # Loop to retry on rate limits
                async with session.get(f"{self.base_url}/{endpoint}", headers=headers, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:
                        # Handle rate limit by checking the Retry-After header
                        retry_after = int(response.headers.get("Retry-After", 5))  # Default to 5 seconds if not provided
                        
                        if retry_after != previous_retry_after:
                            message = f"Rate limit exceeded. Retrying after {retry_after} seconds."
                            if message != last_printed_message:  # Only print if the message has changed
                                # print(message)
                                last_printed_message = message
                        
                        previous_retry_after = retry_after
                        await asyncio.sleep(retry_after)
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
            track_id = track['id']
            artist_id = track['artists'][0]['id']
            
            # Fetch genres for the artist
            artist_info = await self._make_request(f'artists/{artist_id}')
            genres = artist_info.get('genres', [])
            
            # Fetch audio features for the track
            audio_features = await self._make_request(f'audio-features/{track_id}')
            
            # Build the track info dictionary
            track_info = {
                self.config_manager.TRACK_TITLE_COLUMN: track_name,
                self.config_manager.ARTIST_NAME_COLUMN: artist_name,
                'spotify_album': track['album']['name'],
                'spotify_release_date': self.helper.format_date(track['album']['release_date']),
                'spotify_duration_ms': track['duration_ms'],
                'spotify_popularity': track['popularity'],
                'spotify_genres': genres[0] if genres else None,
                'spotify_danceability': audio_features.get('danceability'),
                'spotify_energy': audio_features.get('energy'),
                'spotify_valence': audio_features.get('valence'),
                'spotify_acousticness': audio_features.get('acousticness'),
                'spotify_instrumentalness': audio_features.get('instrumentalness'),
                'spotify_liveness': audio_features.get('liveness'),
                'spotify_speechiness': audio_features.get('speechiness'),
                'spotify_tempo': audio_features.get('tempo'),
                'spotify_mode': audio_features.get('mode'),
                'spotify_loudness': audio_features.get('loudness'),
                'spotify_time_signature': audio_features.get('time_signature')
            }
            return track_info
        else:
            return None
        
    async def fetch_all_track_info(self, df):
        """Fetch Spotify info for all rows in the dataframe."""
        tasks = []
        for row in df.iter_rows(named=True):
            track_name = row[self.config_manager.TRACK_TITLE_COLUMN]
            artist_name = row[self.config_manager.ARTIST_NAME_COLUMN]
            tasks.append(self.get_track_info(track_name, artist_name))
        return await asyncio.gather(*tasks)

    async def process_data(self, df, batch_size=100, delay=5):
        """Fetch track info in batches to avoid API rate limits and return a dataframe with Spotify data."""
        spotify_data = []

        # Split the dataframe into batches
        num_batches = (len(df) + batch_size - 1) // batch_size
        for start in tqdm_asyncio(range(0, len(df), batch_size), desc='Processing batches of SpotifyAPI requests', total=num_batches, unit='batch'):
            batch = df[start:start + batch_size]
            tasks = [self.get_track_info(row[self.config_manager.TRACK_TITLE_COLUMN], 
                                        row[self.config_manager.ARTIST_NAME_COLUMN]) 
                    for row in batch.iter_rows(named=True)]

            # Await and gather all tasks in the current batch
            batch_results = await asyncio.gather(*tasks)
            
            # Filter out None values and add to the main list
            spotify_data.extend([track for track in batch_results if track])

            # Add a delay after each batch to reduce pressure on the API
            await asyncio.sleep(delay)

        # Convert the Spotify data to a DataFrame and merge with the original dataframe
        spotify_df = pl.DataFrame(spotify_data)

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
    'track_title': ['Houdini', 'Please Please Please', 'She Will Be Loved'],
    'artist_name': ['Dua Lipa', 'Sabrina Carpenter', 'Maroon 5']
    })

    asyncio.run(process(df))