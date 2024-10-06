from data_extract.genius_api import Genius

# Example usage
song_title = "Shape of You"
artist_name = "Ed Sheeran"

Genius().get_song_lyrics(song_title, artist_name)
