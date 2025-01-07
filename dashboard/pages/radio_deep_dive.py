import polars as pl
import streamlit as st
from datetime import timedelta

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
    '''
    Resets all page filters:
    - Resets `date_period` to full available range.
    - Resets `genres_selection`.
    - Resets `release_year_range` to full available range.
    '''
    global min_date, max_date, release_years  # Ensure these are accessible

    # Reset date filter
    st.session_state['date_period'] = (min_date, max_date)

    # Reset genre selection
    st.session_state.pop('genres_selection', None)
    st.session_state['select_all_genres'] = True

    # Reset artist selection
    st.session_state.pop('artists_selection', None)
    st.session_state['select_all_artists'] = True

    # Reset release year filter
    if release_years:
        st.session_state['release_year_range'] = (release_years[0], release_years[-1])

if 'radio_name_filter' not in st.session_state:
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
    with st.container(border=True):
        view_option = st.radio(
            label='View plot data by...',
            index=0,
            options=['Artist', 'Track'],
            horizontal=True
        )

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
    with st.expander(label='Filter by :blue[**Genres**]', icon='🎼'):
        # Checkbox for Select All
        filters.update_select_all_checkbox(
            state_key='genres_selection',
            filter_column='Selected?',
            checkbox_key='select_all_genres'
        )

        # Render the checkbox
        st.checkbox(
            'Select All',
            value=True,
            key='select_all_genres',
            # value=select_all_checked,
            on_change=filters.toggle_select_all,
            args=('genres_selection', 'Selected?', 'select_all_genres'),
            help='Genres ordered by importance\n\ni.e Number of plays, in descending order'
        )

        unique_genres = radio_df.select('spotify_genres').unique()
        # Extract unique genres and their count from radio_df
        genre_counts = (
            radio_df
            .group_by('spotify_genres')
            .len()
            .rename({'len': 'genre_count'})  # Rename count column for clarity
        )

        # If 'genres_selection' is not in session_state, initialize it
        # Otherwise refresh it with the selcted radio genres
        if 'genres_selection' not in st.session_state:
            genre_df = (
                unique_genres
                .join(genre_counts, on='spotify_genres', how='left')
                .with_columns(pl.col('genre_count').fill_null(0))
                .sort('genre_count', descending=True)
                .with_columns(pl.lit(True).alias('Selected?'))  # Default selection to True
            )
            st.session_state['genres_selection'] = genre_df
        else:
            # Drop genre_count from session_state before joining to avoid duplication
            existing_selection = st.session_state['genres_selection'].drop(['genre_count'], strict=False)

            # Perform a LEFT JOIN to merge existing selections with new genres
            genre_df = (
                unique_genres
                .join(existing_selection, on='spotify_genres', how='left')
                .join(genre_counts, on='spotify_genres', how='left')
                .with_columns(
                    pl.col('Selected?').fill_null(True),  # Fill missing selections with True
                    pl.col('genre_count').fill_null(0)  # Fill missing counts with 0
                )
                .sort('genre_count', descending=True)  # Sort by descending count
            )

            # Update session_state with new genre_df
            st.session_state['genres_selection'] = genre_df

        if 'select_all_genres' not in st.session_state: 
            st.session_state['select_all_genres'] = True

        st.data_editor(
            st.session_state['genres_selection'].drop(['genre_count'], strict=False),
            use_container_width=True,
            column_config={
                'spotify_genres': {'width': 150},
                'Selected?': {'width': 80},
            },
            key='genre_editor',
            on_change=filters.update_editor_selection_in_session_state,
            args=('genre_editor', 'genres_selection'),
            hide_index=True
        )
    # Filter the dataframe by selected genres
    selected_genres = (
        st.session_state['genres_selection']
        .filter(pl.col('Selected?'))
        ['spotify_genres']
    )
    radio_df = filters.filter_by_list(radio_df, 'spotify_genres', selected_genres.to_list())

    # Artists Filter
    with st.expander(label='Filter by :blue[**Artist**]', icon='🎤'):
        # Checkbox for Select All
        filters.update_select_all_checkbox(
            state_key='artists_selection',
            filter_column='Selected?',
            checkbox_key='select_all_artists'
        )
        # Render the checkbox
        st.checkbox(
            'Select All',
            value=True,
            key='select_all_artists',
            on_change=filters.toggle_select_all,
            args=('artists_selection', 'Selected?', 'select_all_artists'),
            help='Artists ordered by importance\n\ni.e Number of plays, in descending order'
        )

        unique_artists = radio_df.select(cm.ARTIST_NAME_COLUMN).unique()
        # Extract unique artists and their count from radio_df
        artists_counts = (
            radio_df
            .group_by(cm.ARTIST_NAME_COLUMN)
            .len()
            .rename({'len': 'artist_count'})  # Rename count column for clarity
        )

        # If 'artists_selection' is not in session_state, initialize it
        # Otherwise refresh it with the selcted radio artists
        if 'artists_selection' not in st.session_state:
            artists_df = (
                unique_artists
                .join(artists_counts, on=cm.ARTIST_NAME_COLUMN, how='left')
                .with_columns(pl.col('artist_count').fill_null(0))
                .sort('artist_count', descending=True)
                .with_columns(pl.lit(True).alias('Selected?'))  # Default selection to True
            )
            st.session_state['artists_selection'] = artists_df
        else:
            # Drop artiste_count from session_state before joining to avoid duplication
            existing_selection = st.session_state['artists_selection'].drop(['artist_count'], strict=False)

            # Perform a LEFT JOIN to merge existing selections with new artists
            artists_df = (
                unique_artists
                .join(existing_selection, on=cm.ARTIST_NAME_COLUMN, how='left')
                .join(artists_counts, on=cm.ARTIST_NAME_COLUMN, how='left')
                .with_columns(
                    pl.col('Selected?').fill_null(True),  # Fill missing selections with True
                    pl.col('artist_count').fill_null(0)  # Fill missing counts with 0
                )
                .sort('artist_count', descending=True)  # Sort by descending count
            )

            # Update session_state with new artists_df
            st.session_state['artists_selection'] = artists_df

        if 'select_all_artists' not in st.session_state:
            st.session_state['select_all_artists'] = True

        st.data_editor(
            st.session_state['artists_selection'].drop(['artist_count'], strict=False),
            use_container_width=True,
            column_config={
                cm.ARTIST_NAME_COLUMN: {'width': 150},
                'Selected?': {'width': 80},
            },
            key='artists_editor',
            on_change=filters.update_editor_selection_in_session_state,
            args=('artist_editor', 'artists_selection'),
            hide_index=True
        )

    # Filter the dataframe by selected artists
    selected_artists = (
        st.session_state['artists_selection']
        .filter(pl.col('Selected?'))
        [cm.ARTIST_NAME_COLUMN]
    )
    radio_df = filters.filter_by_list(radio_df, cm.ARTIST_NAME_COLUMN, selected_artists.to_list())
    
    
    # Reset settings button
    st.button('Reset Page Settings', on_click=reset_page_settings)

radio_chosen

########################################
## Artist/Track Dataframe with plots  ##
########################################

# Select dimensions based on user choice
if view_option == 'Artist':
    group_cols = [cm.ARTIST_NAME_COLUMN, 'spotify_genres']
else:
    group_cols = [cm.TRACK_TITLE_COLUMN, cm.ARTIST_NAME_COLUMN, 'spotify_genres']

# Use all filtered data for total plays
df_all_time = radio_df

# Compute total plays over the entire date period
total_plays_all = (
    df_all_time
    .group_by(group_cols)
    .agg([
        pl.count().alias('Total Plays'),
    ])
)

# Identify last 30 days from the max date for the sparkline
max_date_in_df = df_all_time[cm.DAY_COLUMN].max()
last_30_days_start = max_date_in_df - timedelta(days=31)
last_30_days_end   = max_date_in_df - timedelta(days=1)
df_30_days = radio_df.filter(
    (pl.col(cm.DAY_COLUMN) >= last_30_days_start)
    & (pl.col(cm.DAY_COLUMN) <= last_30_days_end)
)

# Build the date range for zero-filling
date_series = pl.date_range(
    start=last_30_days_start,
    end=last_30_days_end,
    interval="1d",
    eager=True  # returns a Polars Series directly
)
all_dates = pl.DataFrame({cm.DAY_COLUMN: date_series})

# Cross-join all dates with dimension combos
dim_combos = df_30_days.select(group_cols).unique()
all_combinations = dim_combos.join(all_dates, how='cross')

# Count daily plays within last 30 days
daily_counts_30 = (
    df_30_days
    .group_by(group_cols + [cm.DAY_COLUMN])
    .agg([pl.count().alias('plays_per_day')])
)

# Zero-fill missing dates for the sparkline
zero_filled = (
    all_combinations
    .join(daily_counts_30, on=group_cols + [cm.DAY_COLUMN], how='left')
    .with_columns(pl.col('plays_per_day').fill_null(0))
)

# Collect daily plays into a list for the sparkline
sparkline_df = (
    zero_filled
    .group_by(group_cols)
    .agg([
        pl.col('plays_per_day').sort_by(cm.DAY_COLUMN).alias('plays_list'),
    ])
)

# Combine overall total plays with sparkline info
final_df = (
    sparkline_df
    .join(total_plays_all, on=group_cols, how='left')
    .sort('Total Plays', descending=True)
)

# Compute fraction of max (based on full-period total plays)
max_plays = final_df['Total Plays'].max()
final_df = final_df.with_columns(
    (pl.col('Total Plays') / max_plays).alias('fraction_of_max')
)

# Limit to top 50
final_df = final_df.head(50)

# Configure columns
col_config = {}
if view_option == 'Artist':
    col_config[cm.ARTIST_NAME_COLUMN] = st.column_config.Column(label="Artist", width="small")
    col_config['spotify_genres'] = st.column_config.Column(label="Genre", width="small")
else:
    col_config[cm.TRACK_TITLE_COLUMN] = st.column_config.Column(label="Track Title", width="small")
    col_config[cm.ARTIST_NAME_COLUMN] = st.column_config.Column(label="Artist", width="small")
    col_config['spotify_genres'] = st.column_config.Column(label="Genre", width="small")

col_config["plays_list"] = st.column_config.LineChartColumn(
    label="Daily Plays (Last 30d)",
    width="big",
    help="Zero-filled daily plays for last 30 days"
)

col_config["Total Plays"] = st.column_config.Column(
    label="Total Plays",
    width="small"
)

col_config["fraction_of_max"] = st.column_config.ProgressColumn(
    label="",
    help="Relative share of the highest total plays",
    min_value=0.0,
    max_value=1.0,
    width='big',
    format=" "
)

# Convert to pandas and render
df_to_display = final_df.to_pandas()

st.subheader("30-Day Overview")
st.data_editor(
    df_to_display,
    column_config=col_config,
    hide_index=True,
    use_container_width=True
)



### Graphs
# 1 - Dataframe table with graphs [Done]
# 2 - Bar Chart (possibly not) [Rejected]
# 3 - Most Played per week
# 4 - Histogram by # plays
# 5 - Evolution - Cumultive and non-cumulative
# 6 - Scatterplot Popularity vs Plays
# 7 - Radar Chart for song feelings sound

# Add image of selected radio up top

# Add a right column for the 'average of other radios'
## These should be included in the filters of the sidebar
