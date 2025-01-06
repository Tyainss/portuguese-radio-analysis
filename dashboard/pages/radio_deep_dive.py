import polars as pl
import streamlit as st

from data_extract.config_manager import ConfigManager

from utils import storage, filters

cm = ConfigManager()
app_config = cm.load_json(path='dashboard/app_config.json')

# Load the data
df_radio_data = storage.load_data(cm.RADIO_CSV_PATH, cm.RADIO_SCRAPPER_SCHEMA)
df_artist_info = storage.load_data(cm.ARTIST_INFO_CSV_PATH, cm.ARTIST_INFO_SCHEMA)
df_track_info = storage.load_data(cm.TRACK_INFO_CSV_PATH, cm.TRACK_INFO_SCHEMA)

radio_options = list(app_config.keys())

@st.cache_data
def load_main_df(pandas_format=False):
    return storage.load_joined_data(
        df_radio_data, df_artist_info, df_track_info,
        artist_col=cm.ARTIST_NAME_COLUMN,
        track_col=cm.TRACK_TITLE_COLUMN,
        pandas_format=pandas_format,
    )

df_joined = load_main_df()
df_joined = filters.filter_by_most_recent_min_date(df_joined, cm.RADIO_COLUMN, cm.DAY_COLUMN)


def reset_page_settings():
    """
    Resets all page filters:
    - Resets `date_period` to full available range.
    - Resets `genres_selection`.
    - Resets `release_year_range` to full available range.
    """
    global min_date, max_date, release_years  # Ensure these are accessible

    # Reset date filter
    st.session_state['date_period'] = (min_date, max_date)

    # Reset genre selection
    st.session_state.pop("genres_selection", None)
    st.session_state['select_all_genres'] = True

    # Reset release year filter
    if release_years:
        st.session_state['release_year_range'] = (release_years[0], release_years[-1])

if "radio_name_filter" not in st.session_state:
    st.session_state['radio_name_filter'] = radio_options[0]

# st.session_state.clear()

### Sidebar Filters ###
with st.sidebar:
    st.title(':gear: Page Settings')

    # Radio Filter
    radio_chosen = st.selectbox(
        label='Pick the Radio',
        options=radio_options,
        index=0,
        key='radio_name_filter',
    )
    radio_df = filters.filter_by_radio(
        df_joined, 
        radio_name_col=cm.RADIO_COLUMN,
        radio_selected=app_config[radio_chosen].get('name')
    )
    # radio_df = radio_df.head()


    min_date = radio_df[cm.DAY_COLUMN].min()
    max_date = radio_df[cm.DAY_COLUMN].max()
    if 'date_period' not in st.session_state:
        st.session_state['date_period'] = (min_date, max_date)

    # Date Filter
    new_date_period = st.date_input(
        label=':calendar: Select the time period',
        # value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        key='date_period'
    )
    # If user selected a date range
    if isinstance(new_date_period, tuple) and new_date_period:
        start_date, *end_date = new_date_period
        radio_df = filters.filter_by_date(radio_df, cm.DAY_COLUMN, start_date, end_date[0] if end_date else None)


    # Relase Year Filter
    release_years = (
        radio_df
        .with_columns(pl.col('spotify_release_date').dt.year().cast(pl.Int32).alias('release_year'))  # Convert to Int
        .drop_nulls('release_year')  # Remove None values
        .select('release_year')  # Keep only the release_year column
        .unique()
        .sort('release_year', descending=False)
        ['release_year']
    ).to_list()
    
    if 'release_year_range' not in st.session_state:
        st.session_state['release_year_range'] = (release_years[0], release_years[-1])
    

    st.session_state['release_year_range'] = (
        max(release_years[0], st.session_state['release_year_range'][0]),
        min(release_years[-1], st.session_state['release_year_range'][1])
    )

    # Define the range slider and store its value in session state
    st.slider(
        ':date: Select the range of :blue[**release years**] for the tracks',
        min_value=release_years[0],
        max_value=release_years[-1],
        value=st.session_state['release_year_range'],  # Persist selection
        key='release_year_slider',
        on_change=filters.update_release_year_selection_in_session_state,
        step=1
    )
    # # Unpack selected range from session state
    start_release_year, end_release_year = st.session_state['release_year_range']
    radio_df = filters.filter_by_release_year_range(radio_df, 'spotify_release_date', start_release_year, end_release_year)

    # Genres Filter
    with st.expander(label='Filter by Genres'):
        # Checkbox for Select All
        filters.update_select_all_checkbox(
            state_key="genres_selection",
            filter_column="Selected?",
            checkbox_key="select_all_genres"
        )

        # Render the checkbox
        st.checkbox(
            "Select All",
            key="select_all_genres",
            # value=select_all_checked,
            on_change=filters.toggle_select_all,
            args=("genres_selection", "Selected?", "select_all_genres"),
            help='Genres ordered by importance\n\ni.e Number of plays, in descending order'
        )

        genres = radio_df.select('spotify_genres').unique()
        # Extract unique genres and their count from radio_df
        genre_counts = (
            radio_df
            .group_by("spotify_genres")
            .count()
            .rename({"count": "genre_count"})  # Rename count column for clarity
        )

        # If 'genres_selection' is not in session_state, initialize it
        # Otherwise refresh it with the selcted radio genres
        if 'genres_selection' not in st.session_state:
            genre_df = (
                genres
                .join(genre_counts, on="spotify_genres", how="left")
                .with_columns(pl.col("genre_count").fill_null(0))
                .sort("genre_count", descending=True)
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

        st.data_editor(
            st.session_state["genres_selection"].drop(["genre_count"], strict=False),
            use_container_width=True,
            column_config={
                "spotify_genres": {"width": 150},
                "Selected?": {"width": 80},
            },
            key="genre_editor",
            on_change=filters.update_genre_selection_in_session_state,
            hide_index=True
        )
    # Filter the dataframe by selected genres
    selected_genres = (
        st.session_state["genres_selection"]
        .filter(pl.col("Selected?"))
        ["spotify_genres"]
    )
    radio_df = filters.filter_by_genres(radio_df, "spotify_genres", selected_genres.to_list())
    
    
    # Reset settings button
    st.button('Reset Page Settings', on_click=reset_page_settings)

radio_chosen

df_filtered = radio_df

st.write(df_filtered)

st.write(st.session_state)


st.write("Placeholder for Radio Deep Dive")


# Add a 'Select/Unselect' all button
# Structure file differently to not occupy so much space with filters? Move sidebar filters into another file?