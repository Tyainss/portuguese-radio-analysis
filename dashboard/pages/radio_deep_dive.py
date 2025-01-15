import polars as pl
import streamlit as st
import plotly.express as px
from datetime import timedelta

from data_extract.config_manager import ConfigManager

from utils import storage, filters
from utils.radio_deep_dive import plots
from utils.helper import number_formatter

cm = ConfigManager()
app_config = cm.load_json(path='dashboard/app_config.json')

# Load the data
df_radio_data = storage.load_data(cm.RADIO_CSV_PATH, cm.RADIO_SCRAPPER_SCHEMA)
df_artist_info = storage.load_data(cm.ARTIST_INFO_CSV_PATH, cm.ARTIST_INFO_SCHEMA)
df_track_info = storage.load_data(cm.TRACK_INFO_CSV_PATH, cm.TRACK_INFO_SCHEMA)

radio_options = list(app_config.keys())

# @st.cache_data
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

if 'select_all_genres' not in st.session_state: 
    st.session_state['select_all_genres'] = True

if 'select_all_artists' not in st.session_state:
    st.session_state['select_all_artists'] = True

if 'artist_editor' not in st.session_state:
    st.session_state['artist_editor'] = {}

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

    # Dataframe of the other non-selected radios, for comparison
    other_radios_df = filters.filter_by_radio(
        df_joined, 
        radio_name_col=cm.RADIO_COLUMN,
        radio_selected=app_config[radio_chosen].get('name'),
        exclude=True,
    )

    radio_df = filters.filter_by_radio(
        df_joined, 
        radio_name_col=cm.RADIO_COLUMN,
        radio_selected=app_config[radio_chosen].get('name')
    )
    
    # Sidebar Container for Artist/Track selection
    with st.container(border=True):
        view_option = st.radio(
            label='View plot data by...',
            index=0,
            options=['Artist', 'Track'],
            horizontal=True
        )

    # Get min and max dates
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
        other_radios_df = filters.filter_by_date(other_radios_df, cm.DAY_COLUMN, start_date, end_date[0] if end_date else None)

    # Relase Year Filter
    release_years = (
        radio_df
        .with_columns(pl.col('spotify_release_date').dt.year().cast(pl.Int32).alias('release_year'))
        .drop_nulls('release_year')
        .select('release_year')
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
        value=st.session_state['release_year_range'],
        key='release_year_slider',
        on_change=filters.update_release_year_selection_in_session_state,
        step=1
    )

    # Unpack selected range from session state
    start_release_year, end_release_year = st.session_state['release_year_range']
    radio_df = filters.filter_by_release_year_range(radio_df, 'spotify_release_date', start_release_year, end_release_year)
    other_radios_df = filters.filter_by_release_year_range(other_radios_df, 'spotify_release_date', start_release_year, end_release_year)

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
            key='select_all_genres',
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
            .rename({'len': 'genre_count'})
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
                    pl.col('Selected?').fill_null(True),
                    pl.col('genre_count').fill_null(0)
                )
                .sort('genre_count', descending=True)
            )

            # Update session_state with new genre_df
            st.session_state['genres_selection'] = genre_df

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
    if not st.session_state['select_all_genres']:
        selected_genres = (
            st.session_state['genres_selection']
            .filter(pl.col('Selected?'))
            ['spotify_genres']
        )
        radio_df = filters.filter_by_list(radio_df, 'spotify_genres', selected_genres.to_list())
        other_radios_df = filters.filter_by_list(other_radios_df, 'spotify_genres', selected_genres.to_list())

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
            .rename({'len': 'artist_count'})
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

        st.data_editor(
            st.session_state['artists_selection'].drop(['artist_count'], strict=False),
            use_container_width=True,
            column_config={
                cm.ARTIST_NAME_COLUMN: {'width': 150},
                'Selected?': {'width': 80},
            },
            key='artists_editor',
            on_change=filters.update_editor_selection_in_session_state,
            args=('artists_editor', 'artists_selection'),
            hide_index=True
        )

    # Filter the dataframe by selected artists
    if not st.session_state['select_all_artists']:
        selected_artists = (
            st.session_state['artists_selection']
            .filter(pl.col('Selected?'))
            [cm.ARTIST_NAME_COLUMN]
        )
        radio_df = filters.filter_by_list(radio_df, cm.ARTIST_NAME_COLUMN, selected_artists.to_list())
        other_radios_df = filters.filter_by_list(other_radios_df, cm.ARTIST_NAME_COLUMN, selected_artists.to_list())
    
    
    # Reset settings button
    st.button('Reset Page Settings', on_click=reset_page_settings)

radio_chosen

# Handle empty dataframe scenario
if radio_df.is_empty():
    st.warning("No available data to display.")
else:

    ############################
    ## Artist/Track Sparkline ##
    ############################
    plots.display_sparkline(radio_df, view_option)

    ########################
    ## Comparison section ##
    ########################
    with st.expander('', expanded=True):
        st.markdown(
            """
            <div style="background-color: #edf0f5; padding: 15px; border-radius: 10px;">
                <h2 style="text-align: center; color: #333;">📊 Comparison to Other Radios</h2>
                <p style="font-size: 16px;">
                    Understanding how a selected radio station compares to others is key to identifying trends, uniqueness, 
                    and performance. This section provides a side-by-side comparison of <b>top artists, play distributions, 
                    and genre trends</b>, helping you uncover insights such as:
                </p>
                <ul style="font-size: 16px; margin-left: 20px;">
                    <li>🎵 <b>Which artists/tracks are more popular on this radio compared to others?</b></li>
                    <li>📈 <b>How do play counts vary across different stations?</b></li>
                    <li>🎨 <b>Are there unique genre preferences in this radio’s audience?</b></li>
                </ul>
                <p style="font-size: 16px;">
                    Each visualization below is designed to <b>highlight key differences</b> between the selected radio 
                    and the aggregated performance of other stations.
                </p>
            </div>
            """,
            unsafe_allow_html=True
        )
        st.write('#####')

        # Centered headers for each column
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(
                f"""
                <div style="text-align: center; background-color: #e3e7f1; padding: 8px; 
                            border-radius: 8px; font-size: 20px; font-weight: bold; color: #1f2937;">
                    🎧 {radio_df[cm.RADIO_COLUMN][0]}
                </div>
                """,
                unsafe_allow_html=True
            )

        with col2:
            st.markdown(
                """
                <div style="text-align: center; background-color: #e3e7f1; padding: 8px; 
                            border-radius: 8px; font-size: 20px; font-weight: bold; color: #1f2937;">
                    📡 Other Radios
                </div>
                """,
                unsafe_allow_html=True
            )
        
        st.write('#####')
        plots.display_top_bar_chart(radio_df, view_option, other_radios_df)
        st.divider()
        plots.display_top_by_week_chart(radio_df, view_option, other_radios_df)
        st.divider()
        plots.display_play_count_histogram(radio_df, view_option, other_radios_df)
        st.divider()
        plots.display_popularity_vs_plays_quadrant(radio_df, view_option, other_radios_df)
        st.divider()
        plots.display_top_genres_evolution(radio_df, other_radios_df)


    ######################
    ## Radio Highlights ##
    ######################
    plots.display_underplayed_overplayed_highlights(radio_df, other_radios_df, view_option)
    st.divider()
        

    ########################################
    ## Artist/Track Dataframe with plots  ##
    ########################################
    plots.display_plot_dataframe(radio_df, view_option)




### Graphs
# 0 - Explore other ways of showing several sparklines, 1 by artist, besides the dataframe [Done]
#       - Line chart with top 30/50 artist for last 60d, cumulative or non-cumulative? [Done]
# 1 - Dataframe table with graphs [Done]
# 2 - Bar Chart (possibly not) [Done]
#   - To Improve visually: Try a grading color on the chart. Use the same color for the artist 
#       from the selected radio on the chart for the other radios, this way highlighting them
# 3 - Most Played per week [Done]
# 4 - Histogram by num plays [Done]
# 5 - Evolution - Cumultive and non-cumulative [Done]
# 6 - Scatterplot Popularity vs Plays [Done]
# 7 - Radar Chart for song feelings sound [Rejected]
# 8 - Underplayed and Overplayed songs/artists (add a list of top 5 as "extra") [Done]
#   - Format artists name by removing "acentos" and spaces at the end of their names [Done]
#   - Issue with "Bruno Mars, Lady Gaga" and "Lady Gaga,Bruno Mars"

# Add image of selected radio up top

# If 'release_year_range' wasn't filtered, keep it the min/max for every radio
    # i.e if it's 1997-2024 (min-max) for Cidade and we change to RFM, keep the min-max of RFM
# Same thing for genres filter - If it was filtered, don't had other genres as 'true' when switching between radios


# Should comparison mode only show the same artists/filters/genres/etc from the main radio?
# or if all are selected, show all options from other radios?


# Improve functions comments etc
# Format tooltips