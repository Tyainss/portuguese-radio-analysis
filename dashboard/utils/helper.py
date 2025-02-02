import unidecode
import polars as pl
from datetime import datetime, timedelta

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


def clean_name_column(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """
    Cleans a column by:
    - Stripping spaces.
    - Removing special characters (e.g., "Plutónio" → "Plutonio").
    """
    df = df.with_columns(
        pl.col(col)
        .str.strip_chars()  # Removes leading/trailing spaces
        .map_elements(lambda x: unidecode.unidecode(x) if isinstance(x, str) else x, return_dtype=pl.Utf8)  # Removes accents
        .alias(col)
    )
    
    return df

def week_dates_start_end(week_label):
    year, week = map(int, week_label.split("-W"))
    start_date = datetime.strptime(f"{year}-W{week}-1", "%G-W%V-%u")  # First day of the week
    end_date = start_date + timedelta(days=6)  # Last day of the week
    return [start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")]

def hex_to_rgb(hex_color: str) -> tuple[int, int, int]:
    """
    Converts a hex color string to an RGB tuple.
    
    Args:
        hex_color (str): Color string in hex format (e.g., "#4E87F9").
    
    Returns:
        tuple: A tuple (R, G, B) where each value is an integer between 0 and 255.
    """
    hex_color = hex_color.lstrip('#')
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))