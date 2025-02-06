import polars as pl
import re
import aiohttp
import asyncio
from tqdm.asyncio import tqdm_asyncio

from config_manager import ConfigManager
from helper import Helper
from data_extract import logger

class AsyncSpotifyAPI:
    
    def __init__(self) -> None:
        self.config_manager = ConfigManager()
        self.helper = Helper()
        self.client_id = self.config_manager.SPOTIFY_CLIENT_ID
        self.client_secret = self.config_manager.SPOTIFY_CLIENT_SECRET
        self.token_url = 'https://accounts.spotify.com/api/token'
        self.base_url = 'https://api.spotify.com/v1'
        self.access_token = None
        self.semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent requests

    def _encode_client_credentials(self):
        """Helper function to encode client credentials in base64."""
        import base64
        credentials = f"{self.client_id}:{self.client_secret}"
        return base64.b64encode(credentials.encode()).decode()

    def _clean_artist_name(self, artist_name):
        """
        Truncate the artist name at the first occurrence of any unwanted pattern.
        These include 'Feat.', 'Ft.', ' [' and now ' X ', which are present on radio's artist names.
        """
        # Define the patterns to check
        unwanted_patterns = [
            r"\sfeat\.",
            r"\sFeat\.",
            r"\sft\.",
            r"\sFt\.",
            r"\s\[" ,
            r"\sX\s"
        ]

        for pattern in unwanted_patterns:
            match = re.search(pattern, artist_name)
            if match:
                return artist_name[: match.start()].strip()
        return artist_name  # Return original name if no match

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
                            logger.info(f"Received 502 error. Retrying after {wait_time} seconds.")
                            await asyncio.sleep(wait_time)
                        else:
                            logger.error(f"Failed to authenticate: Status {response.status}")
                            await response.text()
                except aiohttp.ClientConnectorError as e:
                    logger.error(f"Connection error during authentication attempt {attempt + 1}: {e}")
                    await asyncio.sleep(2 ** attempt)
                except Exception as e:
                    logger.error(f"Unexpected error during authentication: {e}")
            raise Exception("Max retries exceeded during authentication")

    async def limited_task(self, func, *args, **kwargs):
        """
        Wrap a task with concurrency control. 
        """
        async with self.semaphore:
            return await func(*args, **kwargs)

    async def _make_request(self, endpoint, params=None):
        """Helper function to make asynchronous GET requests to Spotify API with retry logic."""
        if not self.access_token:
            await self.authenticate()

        headers = {
            'Authorization': f'Bearer {self.access_token}',
        }

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=90)) as session:
            max_attempts = 5
            for attempt in range(max_attempts):
                try:
                    while True:
                        async with session.get(f"{self.base_url}/{endpoint}", headers=headers, params=params) as response:
                            if response.status == 200:
                                return await response.json()

                            elif response.status == 429:
                                # Handle rate limit
                                retry_after = int(response.headers.get("Retry-After", 5))
                                logger.info(f"Rate limit exceeded. Retrying after {retry_after} seconds.")
                                await asyncio.sleep(retry_after)

                            # Retry on 5xx errors (server errors) — ephemeral
                            elif 500 <= response.status < 600:
                                wait_time = 2 ** attempt
                                logger.info(f"Server error {response.status}, retrying in {wait_time} seconds (attempt {attempt+1}).")
                                await asyncio.sleep(wait_time)
                                break  # Break out of the while True to do another for-loop attempt

                            else:
                                # For all other non-200, non-429, non-5xx statuses, raise an exception
                                error_text = await response.text()
                                raise Exception(f"API request failed with status code {response.status}: {error_text}")

                except aiohttp.ClientConnectorError as e:
                    # Retry connection errors
                    if attempt < max_attempts - 1:
                        wait_time = 2 ** attempt
                        logger.info(f"Connection error: {e}. Retrying in {wait_time} seconds (attempt {attempt+1}).")
                        await asyncio.sleep(wait_time)
                    else:
                        raise e
                else:
                    # If we get here without "break" in the while True, it means we successfully returned or raised
                    # If we returned, we're done; if we raised, we won't get here.
                    # So let's end the for-loop if everything went well.
                    break
            else:
                # If we exit the for-loop normally (i.e., no break) -> too many retries
                raise Exception("Max retries exceeded in _make_request")

    async def _process_track_info(self, search_results, track_name, artist_name):
        """Process Spotify track data into a structured format."""
        track = search_results["tracks"]["items"][0]
        track_id = track["id"]
        artist_id = track["artists"][0]["id"]

        # Fetch genres for the artist
        artist_info = await self._make_request(f"artists/{artist_id}")
        genres = artist_info.get("genres", [])

        # Fetch audio features for the track
        # on 27/11/2024 Spotify changed their API to remove access to audio features
        # the approach below tries to still access the audio features, in case they rollback their decision
        try:
            audio_features = await self._make_request(f"audio-features/{track_id}")
        except Exception as e:
            # Check if the error is '403 Forbidden'
            if "403" in str(e):
                logger.info(f"403 Forbidden error for audio-features/{track_id}. Using default values.")
                audio_features = {key: None for key in [
                    "danceability", "energy", "valence", "acousticness", 
                    "instrumentalness", "liveness", "speechiness", "tempo", 
                    "mode", "loudness", "time_signature"
                ]}
            else:
                raise

        # Build the track info dictionary
        return {
            self.config_manager.TRACK_TITLE_COLUMN: track_name,
            self.config_manager.ARTIST_NAME_COLUMN: artist_name,
            "spotify_album": track["album"]["name"],
            "spotify_release_date": self.helper.format_date(track["album"]["release_date"]),
            "spotify_duration_ms": track["duration_ms"],
            "spotify_popularity": track["popularity"],
            "spotify_genres": genres[0] if genres else None,
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
            'spotify_time_signature': audio_features.get('time_signature'),
        }

    async def get_track_info(self, track_name, artist_name):
        """
        Asynchronous function to fetch track information by track name and artist from Spotify.
        Attempting with cleaned artist names if needed.
        """
        # First, ensure that we're authenticated
        if not self.access_token:
            await self.authenticate()

        # Generate the cleaned artist name
        cleaned_artist_name = self._clean_artist_name(artist_name)

        # Try first with the original artist name
        search_params = {
            "q": f"track:{track_name} artist:{artist_name}",
            "type": "track",
            "limit": 1,
        }
        search_results = await self._make_request("search", search_params)

        if not search_results["tracks"]["items"]:
            # If no results, retry with the cleaned artist name
            if cleaned_artist_name != artist_name:
                logger.info(f"Retrying with cleaned artist name: {cleaned_artist_name}")

                search_params["q"] = f"track:{track_name} artist:{cleaned_artist_name}"
                search_results = await self._make_request("search", search_params)

        if search_results["tracks"]["items"]:
            return await self._process_track_info(
                search_results, track_name, artist_name  # ✅ Save the original artist name
            )

        return None
        
    async def fetch_all_track_info(self, df):
        """Fetch Spotify info for all rows in the dataframe."""
        tasks = []
        for row in df.iter_rows(named=True):
            track_name = row[self.config_manager.TRACK_TITLE_COLUMN]
            artist_name = row[self.config_manager.ARTIST_NAME_COLUMN]
            # tasks.append(self.get_track_info(track_name, artist_name))
            tasks.append(self.limited_task(self.get_track_info, track_name, artist_name))
        return await asyncio.gather(*tasks)

    async def process_data(self, df, batch_size=50, delay=10):
        """Fetch track info in batches to avoid API rate limits and return a dataframe with Spotify data."""
        spotify_data = []
        print(df)

        # Split the dataframe into batches
        num_batches = (len(df) + batch_size - 1) // batch_size

        for start in tqdm_asyncio(
            range(0, len(df), batch_size), 
            desc='Processing batches of SpotifyAPI requests', 
            total=num_batches, 
            unit='batch'
        ):
            batch = df[start:start + batch_size]

            tasks = [
                self.limited_task(
                    self.get_track_info,
                    row[self.config_manager.TRACK_TITLE_COLUMN],
                    row[self.config_manager.ARTIST_NAME_COLUMN]
                )
                for row in batch.iter_rows(named=True)
            ]

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
        print(spotify_df[['track_title', 'artist_name', 'spotify_duration_ms', 'spotify_genres']])

    df = pl.DataFrame({
    'track_title': ['Bairro', 'Adore You'],
    'artist_name': ['Wet Bed Gang X Lhast', 'Hugel']
    })

    asyncio.run(process(df))