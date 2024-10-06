import requests
from bs4 import BeautifulSoup

from config_manager import ConfigManager

class Genius:

    def __init__(self, config_path = '../config.json') -> None:
        self.config_manager = ConfigManager(config_path)
        self.access_token = self.config_manager.GENIUS_ACCESS_TOKEN

        # Base URL for Genius API
        self.base_url = "https://api.genius.com"
        # Headers for authorization with the API
        self.headers = {
            'Authorization': f'Bearer {self.access_token}'
        }

    # Function to scrape lyrics from the song's Genius page
    def scrape_lyrics_from_genius_url(self, song_url):
        page = requests.get(song_url)
        soup = BeautifulSoup(page.content, 'html.parser')
        
        # Genius lyrics are inside <div> tags with a specific class
        lyrics_div = soup.find("div", class_="lyrics")
        
        if lyrics_div:
            return lyrics_div.get_text(separator="\n").strip()
        
        # Alternatively, Genius sometimes uses <div> with a custom data-lyrics-container attribute
        lyrics_div = soup.find_all("div", attrs={"data-lyrics-container": "true"})
        if lyrics_div:
            lyrics = "\n".join([div.get_text(separator="\n").strip() for div in lyrics_div])
            return lyrics
        else:
            return None

    # Function to search for a song on Genius
    def search_song_on_genius(self, song_title, artist_name):
        search_url = self.base_url + "/search"
        params = {'q': f'{song_title} {artist_name}'}
        
        # Make a request to the Genius API
        response = requests.get(search_url, headers=self.headers, params=params)
        
        if response.status_code == 200:
            json_data = response.json()
            hits = json_data['response']['hits']
            if hits:
                # Return the first result's song page URL
                return hits[0]['result']['url']
            else:
                return None
        else:
            print("Error in Genius API request:", response.status_code)
            return None

    def get_song_lyrics(self, song_title, artist_name):
        # Search for the song
        song_url = self.search_song_on_genius(song_title, artist_name)

        if song_url:
            print(f"Song URL found: {song_url}")
            
            # Scrape the lyrics
            lyrics = self.scrape_lyrics_from_genius_url(song_url)
            
            if lyrics:
                print("Lyrics found")
                return lyrics
            else:
                print("Could not extract lyrics from the page.")
                return ''
        else:
            print("Song not found on Genius.")
            return ''