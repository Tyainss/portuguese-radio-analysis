from googletrans import Translator
from langdetect import detect
from textblob import TextBlob
from transformers import pipeline, AutoTokenizer

class Lyrics:

    def __init__(self):
        self.translator = Translator()
        self.transformer_model_name = 'cardiffnlp/twitter-roberta-base-emotion'
        self.classifier = pipeline("text-classification", model=self.transformer_model_name, top_k=5)
        self.tokenizer = AutoTokenizer.from_pretrained(self.transformer_model_name)

    def detect_language(self, lyrics):
        try:
            language = detect(lyrics)
            print(f'Detected language: {language}')
            return language
        except Exception as e:
            print('Error detecting language:', e)
            return 'unknown'
    
    def translate_text(self, text, src_lang='pt', dest_lang='en'):
        translation = self.translator.translate(text, src=src_lang, dest=dest_lang)
        return translation
    
    def translate_lyrics(self, lyrics, src_lang=None, dest_lang='en'):
        if not src_lang:
            src_lang = self.detect_language(text=lyrics)

        translation = self.translate_text(text=lyrics, src_lang=src_lang, dest_lang=dest_lang)
        return translation
        
    def count_love_occurrences(self, lyrics):
        love_words = ['love', 'amor', 'amour', 'amore']
        count = sum(lyrics.lower().count(word) for word in love_words)
        print(f'"Love" word count: {count}')
        return count

    def classify_basic_sentiments(self, lyrics):
        """
        Classify Polarity and Subjectivity
        """
        blob = TextBlob(lyrics)
        sentiment = blob.sentiment
        print(f'Sentiment analysis - Polarity: {sentiment.polarity}, Subjectivity: {sentiment.subjectivity}')
        return sentiment

    def _split_lyrics_chunks(self, lyrics, max_length=510):
        """ Split the lyrics into chunks of 510 tokens, this way avoiding limit of the classifier model"""
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
        basic_sentiments = self.classify_basic_sentiments(lyrics)
        complex_sentiments = self.classify_complex_sentiments(lyrics)

        sentiments = {
            'polarity': basic_sentiments.polarity
            , 'subjectivity': basic_sentiments.subjectivity
            , 'joy': complex_sentiments.get('joy')
            , 'sadness': complex_sentiments.get('sadness')
            , 'optimism': complex_sentiments.get('optimism')
            , 'anger': complex_sentiments.get('anger')
        }

        return sentiments