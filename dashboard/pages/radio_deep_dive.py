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

min_release_date = df_joined.with_columns(
    pl.col(cm.SPOTIFY_RELEASE_DATE_COLUMN).dt.year().cast(pl.Int32).alias('release_year')
)['release_year'].min()

max_release_date = df_joined.with_columns(
    pl.col(cm.SPOTIFY_RELEASE_DATE_COLUMN).dt.year().cast(pl.Int32).alias('release_year')
)['release_year'].max()

# Watch for changes to the main radio selection
def update_other_radios():
    st.session_state['other_radios_filter'] = [
        radio for radio in radio_options if radio != st.session_state['radio_name_filter']
    ]

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
        st.session_state['release_year_range'] = (min_release_date, max_release_date)

    update_other_radios()

if 'radio_name_filter' not in st.session_state:
    st.session_state['radio_name_filter'] = radio_options[0]

if 'select_all_genres' not in st.session_state: 
    st.session_state['select_all_genres'] = True

if 'select_all_artists' not in st.session_state:
    st.session_state['select_all_artists'] = True

if 'artist_editor' not in st.session_state:
    st.session_state['artist_editor'] = {}

if 'other_radios_filter' not in st.session_state:
    update_other_radios()



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
        on_change=update_other_radios,
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

    with st.expander(label='Compare to...'):
        st.caption('Select radios to compare with')
        other_radios_chosen = st.segmented_control(
            label='Pick Radio',
            label_visibility='collapsed',
            options=[radio for radio in radio_options if radio != st.session_state['radio_name_filter']],
            selection_mode='multi',
            key='other_radios_filter',
        )
        other_radios_chosen_mapped = [
            app_config[radio].get('name') for radio in other_radios_chosen
        ]
    
    other_radios_df = filters.filter_by_radio(
        df_joined, 
        radio_name_col=cm.RADIO_COLUMN,
        radio_selected=other_radios_chosen_mapped,
    )

    # Get min and max dates
    min_date = df_joined[cm.DAY_COLUMN].min()
    max_date = df_joined[cm.DAY_COLUMN].max()

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
        df_joined = filters.filter_by_date(df_joined, cm.DAY_COLUMN, start_date, end_date[0] if end_date else None)

    # Relase Year Filter
    release_years = (
        df_joined
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
        st.caption("• Marks genres present in the selected radio")
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

        # Combine all genres from selected and other radios
        all_genres = (
            pl.concat([
                radio_df.select('spotify_genres'),
                other_radios_df.select('spotify_genres')
            ])
            .unique()
        )

        # Count total number of plays (rows) per genre across both DFs
        all_genres_count = (
            pl.concat([radio_df, other_radios_df])
            .group_by('spotify_genres')
            .count()
            .rename({'count': 'genre_count'})
        )

        # Determine the radio status for each genre
        radio_genres = set(radio_df.select('spotify_genres').unique().to_series())
        other_genres = set(other_radios_df.select('spotify_genres').unique().to_series())

        # Build a dictionary mapping genres to their statuses
        genre_status_dict = {}
        for genre in radio_genres.union(other_genres):
            if genre in radio_genres and genre in other_genres:
                genre_status_dict[genre] = "both"
            elif genre in radio_genres:
                genre_status_dict[genre] = "only_selected"
            else:
                genre_status_dict[genre] = "only_other"

        STATUS_TO_SYMBOL = {
            "only_selected": "•", #  "⚫",
            "both":         "•", # "⚫",
            "only_other":   "" # "⚪"
        }

        # Create the main genre DataFrame for the filter
        genres_df = (
            all_genres
            # Join total play counts
            .join(all_genres_count, on='spotify_genres', how='left')
            .with_columns(pl.col('genre_count').fill_null(0))
            # Sort by total play count descending
            .sort('genre_count', descending=True)
            # Add a "raw" genre column for filtering purposes
            .with_columns(
                pl.col('spotify_genres').alias('genre_raw')  # Keep the raw genre name
            )
            # Determine each genre's status
            .with_columns(
                pl.col('spotify_genres')
                .replace(genre_status_dict)
                .alias('radio_status')
            )
            # Append the status symbol to the genre name
            .with_columns(
                (
                    pl.col('genre_raw') + " " +
                    pl.col('radio_status').replace(STATUS_TO_SYMBOL)
                ).alias('spotify_genres')
            )
        )

        # Session state logic for managing selection
        if 'genres_selection' not in st.session_state:
            # Initialize with default "Selected?" = True
            st.session_state['genres_selection'] = (
                genres_df
                .with_columns(pl.lit(True).alias('Selected?'))
            )
        else:
            # Merge with existing session state to keep previous selections
            existing_selection = st.session_state['genres_selection'].drop(['genre_count'], strict=False)
            st.session_state['genres_selection'] = (
                genres_df
                .join(existing_selection.select(['genre_raw', 'Selected?']),
                    on='genre_raw', how='left')
                .with_columns(pl.col('Selected?').fill_null(True))
            )

        # Render the genres filter table with just two columns: [Genre, Selected?]
        st.data_editor(
            st.session_state['genres_selection'].select(['spotify_genres', 'Selected?']),
            use_container_width=True,
            column_config={
                'spotify_genres': {'width': 150, 'label': 'Genres'},
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
            ['genre_raw']  # Use the raw genre name for filtering
        )
        # Apply the filters to both dataframes
        radio_df = filters.filter_by_list(radio_df, 'spotify_genres', selected_genres.to_list())
        other_radios_df = filters.filter_by_list(other_radios_df, 'spotify_genres', selected_genres.to_list())


    # Artists Filter
    with st.expander(label='Filter by :blue[**Artist**]', icon='🎤'):
        st.caption("• Marks artists present in the selected radio")
        
        # Checkbox for Select All
        filters.update_select_all_checkbox(
            state_key='artists_selection',
            filter_column='Selected?',
            checkbox_key='select_all_artists'
        )
        st.checkbox(
            'Select All',
            key='select_all_artists',
            on_change=filters.toggle_select_all,
            args=('artists_selection', 'Selected?', 'select_all_artists'),
            help='Artists ordered by importance\n\ni.e Number of plays, in descending order'
        )

        all_artists = (
            pl.concat([
                radio_df.select(cm.ARTIST_NAME_COLUMN),
                other_radios_df.select(cm.ARTIST_NAME_COLUMN)
            ])
            .unique()
        )

        # Count total number of plays (rows) per artist across *both* DFs
        all_artists_count = (
            pl.concat([radio_df, other_radios_df])
            .group_by(cm.ARTIST_NAME_COLUMN)
            .count()
            .rename({"count": "artist_count"})
        )
        radio_artists = set(radio_df.select(cm.ARTIST_NAME_COLUMN).unique().to_series())
        other_artists = set(other_radios_df.select(cm.ARTIST_NAME_COLUMN).unique().to_series())

        # Build a Python dict mapping artist -> status
        status_dict = {}
        for artist in radio_artists.union(other_artists):
            if (artist in radio_artists) and (artist in other_artists):
                status_dict[artist] = "both"
            elif artist in radio_artists:
                status_dict[artist] = "only_selected"
            else:
                status_dict[artist] = "only_other"

        STATUS_TO_SYMBOL = {
            "only_selected": "•", #  "⚫",
            "both":         "•", # "⚫",
            "only_other":   "" # "⚪"
        }
        artists_df = (
            all_artists
            # Join total play counts
            .join(all_artists_count, on=cm.ARTIST_NAME_COLUMN, how="left")
            .with_columns(pl.col("artist_count").fill_null(0))
            # Sort by total play count descending
            .sort("artist_count", descending=True)
            # Add a "raw" artist name column
            .with_columns(
                pl.col(cm.ARTIST_NAME_COLUMN).alias("artist_raw")  # Keep the raw artist name
            )
            # Determine each artist's status
            .with_columns(
                pl.col(cm.ARTIST_NAME_COLUMN)
                .replace(status_dict)
                .alias("radio_status")
            )
            # Now rewrite the original artist column, appending the status symbol
            .with_columns(
                (
                    pl.col("artist_raw") +           # original name
                    pl.lit(" ") +
                    pl.col("radio_status").replace(STATUS_TO_SYMBOL)
                )
                .alias(cm.ARTIST_NAME_COLUMN)
            )
        )

        if 'artists_selection' not in st.session_state:
            # First time: create the default DF with "Selected?" = True
            st.session_state['artists_selection'] = (
                artists_df
                .with_columns(pl.lit(True).alias("Selected?"))
            )
        else:
            # We do a left join to ensure we pick up any newly introduced artists
            existing_selection = st.session_state['artists_selection'].drop(['artist_count'], strict=False)
            st.session_state['artists_selection'] = (
                artists_df
                .join(existing_selection.select([cm.ARTIST_NAME_COLUMN, "Selected?"]),
                    on=cm.ARTIST_NAME_COLUMN,
                    how="left")
                .with_columns(pl.col("Selected?").fill_null(True))
            )

        # Render just the columns we care about: [Artist, Selected?]
        st.data_editor(
            st.session_state['artists_selection'].select([cm.ARTIST_NAME_COLUMN, "Selected?"]),
            use_container_width=True,
            column_config={
                cm.ARTIST_NAME_COLUMN: {
                    "width": 150,
                    "label": 'Artist Name',
                    "help": "• flags artists present in the selected radio"
                    },
                "Selected?": {"width": 80},
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
            ['artist_raw']
        )
        radio_df = filters.filter_by_list(radio_df, cm.ARTIST_NAME_COLUMN, selected_artists.to_list())
        other_radios_df = filters.filter_by_list(other_radios_df, cm.ARTIST_NAME_COLUMN, selected_artists.to_list())
    
    
    # Reset settings button
    st.button('Reset Page Settings', on_click=reset_page_settings)

radio_logo = app_config[radio_chosen].get('logo')
radio_color = app_config[radio_chosen].get('color')

_, logo_col, _ = st.columns([1,1,1])
with logo_col:
    st.image(radio_logo, use_container_width=False)

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
                    <li>🎨 <b>Are there unique genre preferences in this radio's audience?</b></li>
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

        # Create two columns if `other_radios_df` is provided
        if other_radios_df is not None and not other_radios_df.is_empty():
            col1, col2 = st.columns(2)
        else:
            col1 = st.container()
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
        if other_radios_df is not None and not other_radios_df.is_empty():
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
        plots.display_top_bar_chart(radio_df, view_option, other_radios_df, radio_color=radio_color)
        st.divider()
        plots.display_top_by_week_chart(radio_df, view_option, other_radios_df)
        st.divider()
        plots.display_play_count_histogram(radio_df, view_option, other_radios_df, radio_color=radio_color)
        st.divider()
        plots.display_popularity_vs_plays_quadrant(radio_df, view_option, other_radios_df, radio_color=radio_color)
        st.divider()
        plots.display_top_genres_evolution(radio_df, other_radios_df)


    ######################
    ## Radio Highlights ##
    ######################
    if other_radios_df is not None and not other_radios_df.is_empty():
        plots.display_underplayed_overplayed_highlights(radio_df, other_radios_df, view_option)
        st.divider()
        

    ########################################
    ## Artist/Track Dataframe with plots  ##
    ########################################
    plots.display_plot_dataframe(radio_df, view_option)
