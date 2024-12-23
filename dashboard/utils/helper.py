


# Helper function to convert country code or name to flag emoji
def country_to_flag(country: str) -> str:
    # Mapping for common language codes to flag emojis
    language_to_flag = {
        "ar": "🇦🇪",  # Arabic
        "ca": "🇨🇦",  # Catalan
        "de": "🇩🇪",  # German
        "en": "🇺🇸",  # English
        "es": "🇪🇸",  # Spanish
        "fa": "🇮🇷",  # Persian
        "fr": "🇫🇷",  # French
        "hr": "🇭🇷",  # Croatian
        "id": "🇮🇩",  # Indonesian
        "it": "🇮🇹",  # Italian
        "no": "🇳🇴",  # Norwegian
        "pt": "🇵🇹",  # Portuguese
        "ru": "🇷🇺",  # Russian
        "sw": "🇰🇪",  # Swahili
        "tl": "🇵🇭",  # Tagalog
        "tr": "🇹🇷",  # Turkish
        "vi": "🇻🇳",  # Vietnamese
        "unknown": "?"  # Unknown
    }
    return language_to_flag.get(country, country)

def nationality_to_flag(nationality: str) -> str:
    # Mapping for common nationality names to flag emojis
    nationality_to_flag = {
        "Angola": "🇦🇴",
        "Argentina": "🇦🇷",
        "Armenia": "🇦🇲",
        "Australia": "🇦🇺",
        "Austria": "🇦🇹",
        "Belgium": "🇧🇪",
        "Brazil": "🇧🇷",
        "Cabo Verde": "🇨🇻",
        "Canada": "🇨🇦",
        "Chile": "🇨🇱",
        "Colombia": "🇨🇴",
        "Cuba": "🇨🇺",
        "Czechia": "🇨🇿",
        "Denmark": "🇩🇰",
        "Finland": "🇫🇮",
        "France": "🇫🇷",
        "Germany": "🇩🇪",
        "Greece": "🇬🇷",
        "Ireland": "🇮🇪",
        "Italy": "🇮🇹",
        "Jamaica": "🇯🇲",
        "Japan": "🇯🇵",
        "Kenya": "🇰🇪",
        "Latvia": "🇱🇻",
        "Mexico": "🇲🇽",
        "Mozambique": "🇲🇿",
        "Netherlands": "🇳🇱",
        "New Zealand": "🇳🇿",
        "Nigeria": "🇳🇬",
        "Norway": "🇳🇴",
        "Poland": "🇵🇱",
        "Portugal": "🇵🇹",
        "Romania": "🇷🇴",
        "Russian Federation": "🇷🇺",
        "Scotland": "🏴",
        "Senegal": "🇸🇳",
        "South Korea": "🇰🇷",
        "Spain": "🇪🇸",
        "Sweden": "🇸🇪",
        "United Kingdom": "🇬🇧",
        "United States": "🇺🇸",
        "Unknown": "?"
    }
    return nationality_to_flag.get(nationality, nationality)