# utils/filters.py

import polars as pl
import streamlit as st


def filter_by_most_recent_min_date(df: pl.DataFrame, radio_col: str, date_col: str) -> pl.DataFrame:
    """
    Filters the dataframe to only include rows where the date is equal to the most recent
    minimum date among all radios.
    
    Parameters:
        df (pl.DataFrame): The input dataframe containing radio data.
        radio_col (str): The column name for radio identifiers.
        date_col (str): The column name for the date values.
    
    Returns:
        pl.DataFrame: Filtered dataframe.
    """
    # Compute the minimum date for each radio
    min_dates = df.group_by(radio_col).agg(pl.min(date_col).alias("min_date"))

    # Get the most recent minimum date across all radios
    most_recent_min_date = min_dates.select(pl.max("min_date")).item()

    # Filter the original dataframe to include only rows with the most recent minimum date
    return df.filter(pl.col(date_col) >= most_recent_min_date)

def filter_by_radio(df: pl.DataFrame, radio_name_col: str, radio_selected: str) -> pl.DataFrame:
    """
    Return only rows matching the selected radio.
    """
    return df.filter(pl.col(radio_name_col) == radio_selected)

def filter_by_date(df: pl.DataFrame, date_col: str, start_date, end_date=None) -> pl.DataFrame:
    """
    Filters rows where `date_col` is within [start_date, end_date].
    If `end_date` is not provided, filters dates starting from `start_date`.
    """
    condition = pl.col(date_col) >= start_date
    if end_date:
        condition &= pl.col(date_col) <= end_date
    return df.filter(condition)

def filter_by_genres(df: pl.DataFrame, genre_col: str, allowed_genres: list[str]) -> pl.DataFrame:
    """
    Filter the DF so that genre_col is in the allowed_genres list.
    """
    return df.filter(pl.col(genre_col).is_in(allowed_genres))

def update_genre_selection_in_session_state():
    """
    Callback that reads the edited_rows from st.session_state["genre_editor"]
    and applies them to st.session_state["genres_selection"].
    """
    data_editor_state = st.session_state["genre_editor"]
    edited_rows = data_editor_state.get("edited_rows", {})
    if not edited_rows:
        return  # No changes

    # Get the list of booleans from the Polars DF
    selected_col = st.session_state["genres_selection"].get_column("Selected?").to_list()

    # For each row that was changed, update that row in selected_col
    for row_str, changed_fields in edited_rows.items():
        row_idx = int(row_str)
        if "Selected?" in changed_fields:
            selected_col[row_idx] = changed_fields["Selected?"]

    # Overwrite the Polars DF
    updated_df = st.session_state["genres_selection"].with_columns(
        pl.Series("Selected?", selected_col)
    )
    st.session_state["genres_selection"] = updated_df

