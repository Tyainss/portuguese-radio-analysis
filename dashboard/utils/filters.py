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

def update_release_year_selection_in_session_state():
    slider_range = st.session_state['release_year_slider']
    st.session_state['release_year_range'] = slider_range

def filter_by_release_year_range(df: pl.DataFrame, date_col: str, start_year: int, end_year: int) -> pl.DataFrame:
    """
    Filters a dataframe based on a selected range of release years.
    
    If both the min and max years in the dataframe are included in the selected range, 
    it also includes rows where `date_col` is `None`.

    Parameters:
        df (pl.DataFrame): The input dataframe.
        date_col (str): The column containing release dates.
        start_year (int): The starting year for filtering.
        end_year (int): The ending year for filtering.

    Returns:
        pl.DataFrame: Filtered dataframe.
    """
    # Extract min and max available years in the dataframe (excluding None values)
    min_max_years = df.select([
        pl.col(date_col).drop_nulls().dt.year().min().alias("min_year"),
        pl.col(date_col).drop_nulls().dt.year().max().alias("max_year")
    ]).row(0)  # Extracts values as a tuple

    min_year, max_year = min_max_years

    # Create the filtering condition
    year_condition = (pl.col(date_col).dt.year() >= start_year) & (pl.col(date_col).dt.year() <= end_year)

    # If selected range covers full data range, include None values
    if start_year <= min_year and end_year >= max_year:
        return df.filter(year_condition | pl.col(date_col).is_null())

    return df.filter(year_condition)


def update_select_all_checkbox(state_key: str, filter_column: str, checkbox_key: str):
    """
    Updates the 'Select All' checkbox state based on the current selection.

    Parameters:
        state_key (str): The key for the session state dataframe.
        filter_column (str): The name of the column containing selection flags (e.g., 'Selected?').
        checkbox_key (str): The session state key for the 'Select All' checkbox.
    """
    if state_key not in st.session_state:
        return  # No data to update

    df = st.session_state[state_key]

    # Determine current selection state
    all_selected = df[filter_column].all()
    none_selected = not df[filter_column].any()

    # Update the checkbox state accordingly
    st.session_state[checkbox_key] = all_selected

    # return all_selected, none_selected

def toggle_select_all(state_key: str, filter_column: str, checkbox_key: str):
    """
    Toggles selection of all items based on the 'Select All' checkbox state.

    Parameters:
        state_key (str): The key for the session state dataframe.
        filter_column (str): The name of the column containing selection flags (e.g., 'Selected?').
        checkbox_key (str): The session state key for the 'Select All' checkbox.
    """
    if state_key not in st.session_state:
        return  # No data to update

    # Check if 'Select All' is checked or unchecked
    select_all = st.session_state[checkbox_key]

    # Update all rows based on the checkbox state
    st.session_state[state_key] = st.session_state[state_key].with_columns(
        pl.lit(select_all).alias(filter_column)
    )