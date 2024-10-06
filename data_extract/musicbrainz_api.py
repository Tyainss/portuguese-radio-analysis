import requests
import time
import pandas as pd

from helper import Helper

class MusicBrainzAPI:
    
    def __init__(self) -> None:
        self.helper = Helper()

    def fetch_artist_info_from_musicbrainz(self, artist_mbid_list):
        """
        Needs 1 second sleep between every call to not reach API limit
        """
        print('Fetching artist info from MusicBrainz')
        all_artists = []
        for artist_mbid in artist_mbid_list:
            # Sleep 1 second to prevent breaking API limit call per second
            time.sleep(1)
            # MusicBrainz API endpoint for fetching artist information
            url = f'https://musicbrainz.org/ws/2/artist/{artist_mbid}?inc=aliases+tags+ratings+works+url-rels&fmt=json'

            response = requests.get(url)
            data = response.json()

            tags = data.get('tags', '')
            if tags:
                tags.sort(reverse=True, key=lambda x: x['count'])
                main_genre = tags[0].get('name')
            else:
                main_genre = None
            
            country_1 = data.get('country', '')
            country_2 = data.get('area', {}).get('iso-3166-1-codes', [''])[0]
            country_name = self.helper.get_country_name_from_iso_code(country_2 if country_2 else country_1)
            
            career_begin = data.get('life-span', {}).get('begin')
            career_end = data.get('life-span', {}).get('end')
            career_ended = data.get('life-span', {}).get('ended')
            artist_type = data.get('type', '')
            
            # Data treatment on dates to have them in yyyy-MM-DD format
            if career_begin:
                try:
                    career_begin = pd.to_datetime(career_begin).strftime('%Y-%m-%d')
                except:
                    career_begin = ''
            if career_end:
                try:
                    career_end = pd.to_datetime(career_end).strftime('%Y-%m-%d')
                except:
                    career_end = ''
            
            artist_info = {
                'artist_mbid': artist_mbid
                , 'artist_country': country_name
                , 'artist_type': artist_type
                , 'artist_main_genre': main_genre
                , 'artist_career_begin': career_begin
                , 'artist_career_end': career_end
                , 'artist_career_ended': career_ended
            }
            
            all_artists.append(artist_info)
        
        return all_artists