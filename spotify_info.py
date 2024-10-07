from data_extract.spotify_api import Spotify


# Example usage
song_name = "Alô"
artist_name = "Dillaz"
song_info = Spotify().get_track_info(song_name, artist_name)

if song_info:
    print(f"Song Info: {song_info}")
else:
    print("Song not found.")