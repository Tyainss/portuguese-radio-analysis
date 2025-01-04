import polars as pl
import streamlit as st

from data_extract.config_manager import ConfigManager

from utils.storage import (
    load_data
)

cm = ConfigManager()
app_config = cm.load_json(path='dashboard/app_config.json')

# Load the data
df_radio_data = load_data(cm.RADIO_CSV_PATH, cm.RADIO_SCRAPPER_SCHEMA)
df_artist_info = load_data(cm.ARTIST_INFO_CSV_PATH, cm.ARTIST_INFO_SCHEMA)
df_track_info = load_data(cm.TRACK_INFO_CSV_PATH, cm.TRACK_INFO_SCHEMA)

radio_options = list(app_config.keys())

@st.cache_data
def load_df(pandas_format=False):
    df = df_radio_data.join(
            df_artist_info, 
            on=cm.ARTIST_NAME_COLUMN,
            how='left',
        ).join(
            df_track_info,
            on=[cm.TRACK_TITLE_COLUMN, cm.ARTIST_NAME_COLUMN],
            how='left',
        )
    if pandas_format:
        df = df.to_pandas()
    return df

@st.cache_data
def filter_dataframe_by_radio(_df, radio_name):
    return _df.filter(pl.col(cm.RADIO_COLUMN) == radio_name)

df_joined = load_df()

if "radio_name_filter" not in st.session_state:
    st.session_state['radio_name_filter'] = radio_options[0]


# st.session_state.clear()
def filter_genre():
    """
    Reads 'edited_rows' from st.session_state['genre_editor']
    and applies them to the polars_df input. Then saves back to session_state.
    """
    # 1. Grab the changes from the Data Editor state
    data_editor_state = st.session_state["genre_editor"]
    edited_rows = data_editor_state.get("edited_rows", {})
    if not edited_rows:
        return  # No edits, so do nothing

    # 2. Pull the existing 'Selected?' column into a Python list so we can mutate by index
    selected_col = st.session_state["genres_selection"].get_column("Selected?").to_list()
    # 3. For each row that was changed, update that row in selected_col
    for row_str, changed_fields in edited_rows.items():
        row_idx = int(row_str)
        # If the user edited the "Selected?" checkbox...
        if "Selected?" in changed_fields:
            selected_col[row_idx] = changed_fields["Selected?"]

    # 4. Overwrite the Polars DF with updated values
    updated_df = st.session_state["genres_selection"].with_columns(pl.Series("Selected?", selected_col))
    st.session_state["genres_selection"] = updated_df

    # st.rerun()
    # st.session_state.clear()

def reset_settings():
    st.session_state['date_period'] = (min_date, max_date)
    st.session_state.pop("genres_selection", None)


# Sidebar Filters
with st.sidebar:
    st.title(':gear: Page Settings')
    # st.session_state['date_period']
    # Radio Filter
    radio_chosen = st.selectbox(
        label='Pick the Radio',
        options=radio_options,
        index=0,
        key='radio_name_filter',
    )
    radio_df = filter_dataframe_by_radio(df_joined, app_config[radio_chosen].get('name'))
    # radio_df = radio_df.head()

    min_date = radio_df[cm.DAY_COLUMN].min()
    max_date = radio_df[cm.DAY_COLUMN].max()

    # Date Filter
    new_date_period = st.date_input(
        label=':calendar: Select the time period',
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        key='date_period'
    )

    # Radio Filter
    genres = radio_df.select('spotify_genres').unique()
    # Extract unique genres and their count from radio_df
    genre_counts = (
        radio_df
        .group_by("spotify_genres")
        .count()
        .rename({"count": "genre_count"})  # Rename count column for clarity
    )

    # If 'genres_selection' is not in session_state, initialize it
    if 'genres_selection' not in st.session_state:
        genre_df = (
            genres
            .join(genre_counts, on="spotify_genres", how="left")
            .with_columns(pl.col("genre_count").fill_null(0))  # Fill missing counts with 0
            .sort("genre_count", descending=True)  # Sort by count descending
            .with_columns(pl.lit(True).alias("Selected?"))  # Default selection to True
        )
        st.session_state["genres_selection"] = genre_df
    else:
        # Drop genre_count from session_state before joining to avoid duplication
        existing_selection = st.session_state["genres_selection"].drop(["genre_count"], strict=False)

        # Perform a LEFT JOIN to merge existing selections with new genres
        genre_df = (
            genres
            .join(existing_selection, on="spotify_genres", how="left")
            .join(genre_counts, on="spotify_genres", how="left")
            .with_columns(
                pl.col("Selected?").fill_null(True),  # Fill missing selections with True
                pl.col("genre_count").fill_null(0)  # Fill missing counts with 0
            )
            .sort("genre_count", descending=True)  # Sort by descending count
        )

        # Update session_state with new genre_df
        st.session_state["genres_selection"] = genre_df

    # st.session_state
    with st.expander(label='Filter by Genres'):
        st.data_editor(
            st.session_state["genres_selection"].drop(["genre_count"], strict=False),
            use_container_width=True,
            column_config={
                "spotify_genres": {"width": 150},
                "Selected?": {"width": 100},
            },
            # 3) Provide a key so Streamlit can track the widget state
            key="genre_editor",
            on_change=filter_genre,
            hide_index=True
        )

radio_chosen
# new_date_period

# st.write(radio_df[cm.DAY_COLUMN].min())
# st.session_state['date_period']

# Apply Date Filter Only After Both Dates Are Selected
if len(new_date_period) == 2:
    start_date, end_date = new_date_period
    df_filtered = radio_df.filter(
        (pl.col(cm.DAY_COLUMN) >= start_date) & (pl.col(cm.DAY_COLUMN) <= end_date)
    )
elif len(new_date_period) == 1:
    start_date = new_date_period[0]
    df_filtered = radio_df.filter(
        pl.col(cm.DAY_COLUMN) >= start_date
    )
else:
    df_filtered = radio_df  # Keep unfiltered data if only one date is selected

# Filter the dataframe by selected genres
selected_genres = (
    st.session_state["genres_selection"]
    .filter(pl.col("Selected?"))
    ["spotify_genres"]
)

df_filtered  = df_filtered.filter(
    pl.col('spotify_genres').is_in(selected_genres)
)

st.write(df_filtered)

st.write(st.session_state)


st.write("Placeholder for Radio Deep Dive")



# Add a 'Select/Unselect' all button
# Add reset filters button
# Structure file differently to not occupy so much space with filters? Move sidebar filters into another file?