import polars as pl
from deep_translator import GoogleTranslator
from langdetect import detect
from textblob import TextBlob
from transformers import pipeline, AutoTokenizer
from tqdm import tqdm

from genius_api import GeniusAPI
from config_manager import ConfigManager

class LyricsAnalyzer:

    def __init__(self):
        self.config_manager = ConfigManager()
        self.genius = GeniusAPI()
        self.translator = GoogleTranslator()
        self.transformer_model_name = 'cardiffnlp/twitter-roberta-base-emotion'
        self.classifier = pipeline("text-classification", model=self.transformer_model_name, top_k=5)
        self.tokenizer = AutoTokenizer.from_pretrained(self.transformer_model_name)

    def detect_language(self, lyrics):
        try:
            language = detect(lyrics)
            return language
        except Exception as e:
            print('Error detecting language:', e)
            return 'unknown'
    
    def translate_text(self, text, src_lang='pt', dest_lang='en'):
        if not text or not isinstance(text, str):
            print('Invalid input for translation. Returning original text.')
            return text  # Return the original text if input is invalid

        try:
            translation = self.translator.translate(text, source=src_lang, target=dest_lang)
            return translation
        except Exception as e:
            print('Error during translation:', e)
            return text
    
    def translate_lyrics(self, lyrics, src_lang=None, dest_lang='en'):
        if not src_lang:
            src_lang = self.detect_language(lyrics)

        translation = self.translate_text(text=lyrics, src_lang=src_lang, dest_lang=dest_lang)
        return translation
        
    def count_love_occurrences(self, lyrics):
        """
        Count occurrences of 'love' in various languages and conjugations.
        """
        love_words = [
            'love', 'amor', 'amour', 'amore', 'amo', 'amar', 'amando', 'amado',
            'lieben', 'liebe', 'querer', 'quiero', 'adoro', 'adorar'
        ]
        count = sum(lyrics.lower().count(word) for word in love_words)
        return count

    def classify_basic_sentiments(self, lyrics):
        """
        Classify Polarity and Subjectivity
        """
        blob = TextBlob(lyrics)
        sentiment = blob.sentiment
        return sentiment

    def _split_lyrics_chunks(self, lyrics, max_length=500):
        """ Split the lyrics into chunks of 500 tokens, this way avoiding limit of the classifier model"""
        tokens = self.tokenizer(lyrics, return_tensors='pt', truncation=False).input_ids[0]
        for i in range(0, len(tokens), max_length):
            yield self.tokenizer.decode(tokens[i:i + max_length], skip_special_tokens=True)

    def classify_complex_sentiments(self, lyrics):
        """ 
        Classify lyrics in regards for 4 sentiments, grading them from 0 to 100.
        Uses transformer model cardiffnlp/twitter-roberta-base-emotion

        Sentiments:
            - Joy
            - Sadness
            - Optimist
            - Anger        
        """
        emotion_scores = {}
        num_chunks = 0
        for chunk in self._split_lyrics_chunks(lyrics):
            results = self.classifier(chunk)
            num_chunks += 1
            for result in results[0]:
                label = result['label']
                score = result['score']
                emotion_scores[label] = emotion_scores.get(label, 0) + score
                
        # Average the scores over the chunks
        for emotion in emotion_scores:
            emotion_scores[emotion] /= num_chunks
            emotion_scores[emotion] *= 100
            
        return emotion_scores
    
    def classify_lyric_sentiments(self, lyrics):
        english_lyrics = self.translate_lyrics(lyrics) if self.detect_language(lyrics) != 'en' else lyrics

        basic_sentiments = self.classify_basic_sentiments(english_lyrics) # TextBlob seems to only work well with english lyrics
        complex_sentiments = self.classify_complex_sentiments(lyrics)

        sentiments = {
            'lyrics_polarity': basic_sentiments.polarity
            , 'lyrics_subjectivity': basic_sentiments.subjectivity
            , 'lyrics_joy': complex_sentiments.get('joy')
            , 'lyrics_sadness': complex_sentiments.get('sadness')
            , 'lyrics_optimism': complex_sentiments.get('optimism')
            , 'lyrics_anger': complex_sentiments.get('anger')
        }

        return sentiments

    def process_data(self, df):
        """
        Process a DataFrame of track titles and artist names, and returns a DataFrame
        containing information about the lyrics, such as sentiment analysis and its language
        """
        lyrics_info_list = []

        for row in tqdm(df.iter_rows(named=True), total=len(df), desc='Processing Lyrics', unit='track'):
            track_title = row[self.config_manager.TRACK_TITLE_COLUMN]
            artist_name = row[self.config_manager.ARTIST_NAME_COLUMN]
            lyrics = self.genius.get_song_lyrics(song_title=track_title, artist_name=artist_name)

            # Skip processing if no lyrics are found
            if not lyrics:
                print(f"No lyrics found for '{track_title}' by '{artist_name}'. Skipping...")
                lyrics_info_list.append({
                    'lyrics_polarity': None,
                    'lyrics_subjectivity': None,
                    'lyrics_joy': None,
                    'lyrics_sadness': None,
                    'lyrics_optimism': None,
                    'lyrics_anger': None,
                    'lyrics_language': 'unknown',
                    'lyrics_love_occurrences': 0,
                })
                continue

            sentiments = self.classify_lyric_sentiments(lyrics)
            lyrics_info = {
                'lyrics_language': self.detect_language(lyrics)
                , 'lyrics_love_occurrences': self.count_love_occurrences(lyrics)
                }
            
            lyrics_info = sentiments | lyrics_info # Append both dictionaries

            lyrics_info_list.append(lyrics_info)
        
        lyrics_info_df = pl.DataFrame(lyrics_info_list)

        result_df = pl.concat([df, lyrics_info_df], how='horizontal')
        return result_df

if __name__ == '__main__':
    la = LyricsAnalyzer()

    df = pl.DataFrame({
        'artist_name': ['Sabrina Carpenter', 'Billie Eilish', 'Ed Sheeran'],
        'track_title': ['Please Please Please', 'BIRDS OF A FEATHER', 'Shape of You']
    })

    results = la.process_data(df)
    print(results)