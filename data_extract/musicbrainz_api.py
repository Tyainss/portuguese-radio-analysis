import requests
import time
from datetime import datetime
import polars as pl  # Assuming you are using Polars DataFrame

from helper import Helper

class MusicBrainzAPI:
    
    def __init__(self) -> None:
        self.helper = Helper()


    def search_artist_by_name(self, artist_name):
        """
        Search for an artist in MusicBrainz by their name and return their mbid.
        """
        print(f"Searching for artist: {artist_name}")
        url = "https://musicbrainz.org/ws/2/artist/"
        params = {
            'query': f'artist:{artist_name}',
            'fmt': 'json',
            'limit': 1  # Return the first match
        }
        
        response = requests.get(url, params=params)
        data = response.json()
        
        if 'artists' in data and data['artists']:
            # Extract the first artist's mbid
            artist_mbid = data['artists'][0]['id']
            return artist_mbid
        else:
            print(f"No artist found for name: {artist_name}")
            return None

    def fetch_artist_info_by_mbid(self, artist_mbid):
        """
        Fetch detailed artist information from MusicBrainz by their mbid.
        """
        url = f'https://musicbrainz.org/ws/2/artist/{artist_mbid}?inc=aliases+tags+ratings+works+url-rels&fmt=json'
        time.sleep(1)  # To avoid rate limits
        response = requests.get(url)
        data = response.json()

        # Extract artist details
        tags = data.get('tags', [])
        if tags:
            tags.sort(reverse=True, key=lambda x: x['count'])
            main_genre = tags[0].get('name')
        else:
            main_genre = None

        country_1 = data.get('country', '')
        area = data.get('area')
        country_2 = area.get('iso-3166-1-codes', [''])[0] if area else None
        country_name = self.helper.get_country_name_from_iso_code(country_2 if country_2 else country_1)
        
        life_span = data.get('life-span', {})
        career_begin = self.helper.format_date(life_span.get('begin'))
        career_end = self.helper.format_date(life_span.get('end'))
        career_ended = life_span.get('ended')
        artist_type = data.get('type', '')

        return {
            'mb_artist_country': country_name,
            'mb_artist_main_genre': main_genre,
            'mb_artist_type': artist_type,
            'mb_artist_career_begin': career_begin,
            'mb_artist_career_end': career_end,
            'mb_artist_career_ended': career_ended
        }

    def fetch_artist_info_by_name(self, artist_name):
        """
        Fetch detailed artist information from MusicBrainz by the artist name.
        """
        # Step 1: Search for the artist by name and get their mbid
        artist_mbid = self.search_artist_by_name(artist_name)
        
        # Step 2: If mbid is found, fetch the artist info using mbid
        if artist_mbid:
            artist_info = self.fetch_artist_info_by_mbid(artist_mbid)
            return artist_info
        else:
            return None

    def process_data(self, df):
        """
        Process a DataFrame containing artist names and return a DataFrame with extra artist info.
        """
        artist_info_list = []

        for artist_name in df['artist_name']:  # Assuming the column name is 'artist_name'
            artist_info = self.fetch_artist_info_by_name(artist_name)
            if artist_info:
                artist_info_list.append(artist_info)
            else:
                # Append None values for columns if no info is found
                artist_info_list.append({
                    'mb_artist_country': None,
                    'mb_artist_main_genre': None,
                    'mb_artist_type': None,
                    'mb_artist_career_begin': None,
                    'mb_artist_career_end': None,
                    'mb_artist_career_ended': None
                })

        # Convert the list of artist info into a DataFrame
        artist_info_df = pl.DataFrame(artist_info_list)

        # Concatenate the original DataFrame with the new artist info DataFrame
        result_df = pl.concat([df, artist_info_df], how='horizontal')
        return result_df


# Example usage
if __name__ == "__main__":
    mb_api = MusicBrainzAPI()

    # Create a sample DataFrame with artist names
    df = pl.DataFrame({
        'artist_name': ['Radiohead', 'Coldplay', 'Adele']
    })

    # Process the data and fetch artist info
    result_df = mb_api.process_data(df)
    print(result_df)
