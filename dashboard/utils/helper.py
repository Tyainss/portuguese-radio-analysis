


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
