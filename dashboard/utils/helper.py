language_full_name_dict = {
    "ar": "Arabic",
    "ca": "Catalan",
    "de": "German",
    "en": "English",
    "es": "Spanish",
    "fa": "Persian",
    "fr": "French",
    "hr": "Croatian",
    "id": "Indonesian",
    "it": "Italian",
    "no": "Norwegian",
    "pt": "Portuguese",
    "ru": "Russian",
    "sw": "Swahili",
    "tl": "Tagalog",
    "tr": "Turkish",
    "vi": "Vietnamese",
    "unknown": "?",
}

language_to_flag_dict = {
    "ar": "AR",  # Arabic
    "ca": "CA",  # Catalan
    "de": "DE",  # German
    "en": "EN",  # English
    "es": "ES",  # Spanish
    "fa": "FA",  # Persian
    "fr": "FR",  # French
    "hr": "HR",  # Croatian
    "id": "ID",  # Indonesian
    "it": "IT",  # Italian
    "no": "NO",  # Norwegian
    "pt": "PT",  # Portuguese
    "ru": "RU",  # Russian
    "sw": "SW",  # Swahili
    "tl": "YL",  # Tagalog
    "tr": "TR",  # Turkish
    "vi": "VI",  # Vietnamese
    "unknown": "?"  # Unknown
}

nationality_to_flag_dict = {
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
    "Puerto Rico": "🇵🇷",
    "Romania": "🇷🇴",
    "Russian Federation": "🇷🇺",
    "Scotland": "🏴",
    "Senegal": "🇸🇳",
    "South Korea": "🇰🇷",
    "Korea, Republic of": "🇰🇷",
    "Spain": "🇪🇸",
    "Sweden": "🇸🇪",
    "United Kingdom": "🇬🇧",
    "United States": "🇺🇸",
    "United States of America": "🇺🇸",
    "Unknown": "?"
}

flag_to_nationality_dict = {
    "🇦🇴": "Angola",
    "🇦🇷": "Argentina",
    "🇦🇲": "Armenia",
    "🇦🇺": "Australia",
    "🇦🇹": "Austria",
    "🇧🇪": "Belgium",
    "🇧🇷": "Brazil",
    "🇨🇻": "Cabo Verde",
    "🇨🇦": "Canada",
    "🇨🇱": "Chile",
    "🇨🇴": "Colombia",
    "🇨🇺": "Cuba",
    "🇨🇿": "Czechia",
    "🇩🇰": "Denmark",
    "🇫🇮": "Finland",
    "🇫🇷": "France",
    "🇩🇪": "Germany",
    "🇬🇷": "Greece",
    "🇮🇪": "Ireland",
    "🇮🇹": "Italy",
    "🇯🇲": "Jamaica",
    "🇯🇵": "Japan",
    "🇰🇪": "Kenya",
    "🇰🇷": "South Korea",
    "🇱🇻": "Latvia",
    "🇲🇽": "Mexico",
    "🇲🇿": "Mozambique",
    "🇳🇱": "Netherlands",
    "🇳🇿": "New Zealand",
    "🇳🇬": "Nigeria",
    "🇳🇴": "Norway",
    "🇵🇱": "Poland",
    "🇵🇹": "Portugal",
    "🇵🇷": "Puerto Rico",
    "🇷🇴": "Romania",
    "🇷🇺": "Russian Federation",
    "🇸🇳": "Senegal",
    "🇪🇸": "Spain",
    "🇸🇪": "Sweden",
    "🇬🇧": "United Kingdom",
    "🇺🇸": "United States of America",
    "?": "Unknown"
}


# Helper function to convert country code or name to flag emoji
def country_to_flag(country: str) -> str:
    # Mapping for common language codes to flag emojis
    return language_to_flag_dict.get(country, country)

def nationality_to_flag(nationality: str) -> str:
    # Mapping for common nationality names to flag emojis
    return nationality_to_flag_dict.get(nationality, nationality)

def number_formatter(number, decimal_places: int = 2) -> str:
    """
    Formats a number with a comma as a thousand separator.

    Parameters:
    number (int, float, or str): The number to format.

    Returns:
    str: The formatted number as a string with comma separators.
    """
    try:
        # Convert the input to float to handle both integers and floats
        number = float(number)
        # Format the number with comma separator
        return f"{number:,.0f}" if number.is_integer() else f"{number:,.{decimal_places}f}"
    except (ValueError, TypeError):
        raise ValueError("Input must be a valid number.")
