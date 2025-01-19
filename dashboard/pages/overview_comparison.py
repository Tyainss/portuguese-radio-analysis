import polars as pl
import streamlit as st

from data_extract.config_manager import ConfigManager

from utils import storage, filters, calculations
from utils.overview_comparison import mappings, plots


cm = ConfigManager()
app_config = cm.load_json(path='dashboard/app_config.json')

# Load the data
df_radio_data = storage.load_data(cm.RADIO_CSV_PATH, cm.RADIO_SCRAPPER_SCHEMA)
df_artist_info = storage.load_data(cm.ARTIST_INFO_CSV_PATH, cm.ARTIST_INFO_SCHEMA)
df_track_info = storage.load_data(cm.TRACK_INFO_CSV_PATH, cm.TRACK_INFO_SCHEMA)

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

min_date = df_joined[cm.DAY_COLUMN].min()
max_date = df_joined[cm.DAY_COLUMN].max()
min_release_date = df_joined.with_columns(pl.col(cm.SPOTIFY_RELEASE_DATE_COLUMN).dt.year().cast(pl.Int32).alias('release_year'))['release_year'].min()
max_release_date = df_joined.with_columns(pl.col(cm.SPOTIFY_RELEASE_DATE_COLUMN).dt.year().cast(pl.Int32).alias('release_year'))['release_year'].max()


if 'date_period' not in st.session_state:
    st.session_state['date_period'] = (min_date, max_date)
if 'ts_graph' not in st.session_state:
    st.session_state['ts_graph'] = 'Avg Tracks'

def reset_settings():
    st.session_state['date_period'] = (min_date, max_date)
    st.session_state['ts_graph'] = 'Avg Tracks'
    st.session_state['metric_type'] = 'Unique'
    # Reset release year filter
    if release_years:
        st.session_state['release_year_range'] = (min_release_date, max_release_date)

# Sidebar Settings
with st.sidebar:
    st.title(':gear: Page Settings')

    new_date_period = st.date_input(
        label=':calendar: Select the time period',
        # value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        key='date_period'
    )
    new_graph_option = st.radio(
        label='📈 Select :blue-background[**Time Series**] to display:',
        options=['Avg Tracks', 'Avg Hours Played', 'Avg Popularity'],
        index=0,
        key='ts_graph'
    )

    metric_type_option = st.radio(
        label='📊 Select :blue-background[**Metric**] Type',
        options=['Unique', 'Total', 'Average'],
        index=0,
        horizontal=False,
        key='metric_type',
        help="""Display either unique or total combinations, or average.
            \nApplies for :red-background[**Tracks**] and :red-background[**Artists**] metrics"""
    )

    # If user selected a date range
    if isinstance(new_date_period, tuple) and new_date_period:
        start_date, *end_date = new_date_period
        df_joined = filters.filter_by_date(df_joined, cm.DAY_COLUMN, start_date, end_date[0] if end_date else None)

    # Relase Year Filter
    release_years = (
        df_joined
        .with_columns(pl.col(cm.SPOTIFY_RELEASE_DATE_COLUMN).dt.year().cast(pl.Int32).alias('release_year'))
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
    df_joined = filters.filter_by_release_year_range(df_joined, 'spotify_release_date', start_release_year, end_release_year)

    # Reset settings button
    st.button('Reset Page Settings', on_click=reset_settings)


# Calculate global min and max values
metrics = ['avg_tracks', 'avg_time_played', 'avg_popularity']
metric_ranges = {}
global_max_mean_values = {}  # Dictionary to store global max values for each metric

# Initialize config for each radio
for i, (key, val) in enumerate(app_config.items()):
    radio_name = val.get('name')
    app_config[key]['radio_df'] = df_joined.filter(
            pl.col(cm.RADIO_COLUMN) == val.get('name')
        )
    app_config[key]['radio_csv'] = storage.generate_csv(app_config[key]['radio_df'])
    for metric in metrics:
        weekday_metric_df = calculations.prepare_weekday_metrics(app_config[key]['radio_df'], metric=metric, id=radio_name)
        hour_metric_df = calculations.prepare_hourly_metrics(app_config[key]['radio_df'], metric=metric, id=radio_name)
        if metric not in metric_ranges:
            metric_ranges[metric] = {
                'weekday': {'min': float('inf'), 'max': float('-inf')},
                'hour': {'min': float('inf'), 'max': float('-inf')},
            }
        
        # Update weekday min/max if data exists
        if not weekday_metric_df.is_empty():
            weekday_min = weekday_metric_df[metric].min()
            weekday_max = weekday_metric_df[metric].max()

            if weekday_min is not None:
                metric_ranges[metric]['weekday']['min'] = min(
                    metric_ranges[metric]['weekday']['min'], weekday_min
                )
            if weekday_max is not None:
                metric_ranges[metric]['weekday']['max'] = max(
                    metric_ranges[metric]['weekday']['max'], weekday_max
                )

        # Update hour min/max if data exists
        if not hour_metric_df.is_empty():
            hour_min = hour_metric_df[metric].min()
            hour_max = hour_metric_df[metric].max()

            if hour_min is not None:
                metric_ranges[metric]['hour']['min'] = min(
                    metric_ranges[metric]['hour']['min'], hour_min
                )
            if hour_max is not None:
                metric_ranges[metric]['hour']['max'] = max(
                    metric_ranges[metric]['hour']['max'], hour_max
                )

    # Compute mean values for the current radio
    mean_values = app_config[key]['radio_df'].select(
        "lyrics_joy", "lyrics_sadness", "lyrics_optimism", "lyrics_anger", "lyrics_love_occurrences"
    ).mean().to_dict(as_series=False)
    app_config[key]['mean_values'] = mean_values  # Store mean values for this radio
    # Update global max mean values
    if not global_max_mean_values:
        global_max_mean_values = mean_values.copy()
    else:
        for metric, mean_value in mean_values.items():
            global_max_mean_values[metric] = max(global_max_mean_values[metric], mean_value)


mapped_metric_type = mappings.metric_type_map.get(st.session_state['metric_type'])
selected_metric = mappings.graph_metric_map[st.session_state['ts_graph']]

########################
## Header KPIs + Logo ##
########################
ncols = len(app_config)

plots.display_header_kpis(app_config=app_config, ncols=ncols)


#######################
## Time Series Plots ##
#######################
expander = st.expander(label=f'Time Series plots', expanded=True, icon='📈')
with expander:
    
    ###################
    ## Hourly Graphs ##
    ###################
    plots.display_hourly_graph(
        app_config=app_config, 
        ncols=ncols,
        selected_metric=selected_metric,
        metric_ranges=metric_ranges,
    )

    ####################
    ## Weekday Graphs ##
    ####################
    plots.display_weekly_graph(
        app_config=app_config, 
        ncols=ncols,
        selected_metric=selected_metric,
        metric_ranges=metric_ranges,
    )


######################
## Track Statistics ##
######################
plots.display_track_kpis(app_config=app_config, ncols=ncols)


# Track Plots Expander
track_plots_expander = st.expander(label=f'Track Plots', expanded=True, icon='📊')
with track_plots_expander:
    # with stylable_container(
    #     key='track_plots_settings',
    #     css_styles="""
    #         button {
    #             width: 150px;
    #             height: 60px;
    #             background-color: green;
    #             color: white;
    #             border-radius: 5px;
    #             white-space: nowrap;
    #         }
    #         """,
    # ):
    with st.popover(label='Settings', icon='⚙️', use_container_width=False):
        num_languages = st.number_input(
            label='Top Number of Languages',
            value=4,
            min_value=1,
            max_value=10,
            step=1,
        )

    ####################
    ## Track Language ##
    ####################
    plots.display_track_languages(
        app_config=app_config, 
        ncols=ncols,
        num_languages=num_languages,
        mapped_metric_type=mapped_metric_type,
    )

    ##################
    ## Track Decade ##
    ##################
    plots.display_track_decades(
        app_config=app_config, 
        ncols=ncols,
        metric_type_option=metric_type_option,
        mapped_metric_type=mapped_metric_type,
    )


#######################
## Artist Statistics ##
#######################
plots.display_artist_kpis(app_config=app_config, ncols=ncols)

# Artist Plots Expander
artist_plots_expander = st.expander(label=f'Artists Plots', expanded=True, icon='📊')
with artist_plots_expander:
    with st.popover(label='Settings', icon='⚙️', use_container_width=False):
        num_countries = st.number_input(
            label='Top Number of Countries',
            value=5,
            min_value=1,
            max_value=10,
            step=1,
        )

    ####################
    ## Artist Country ##
    ####################
    plots.display_artist_countries(
        app_config=app_config, 
        ncols=ncols,
        num_countries=num_countries,
        mapped_metric_type=mapped_metric_type,
    )
    
    ###################
    ## Artist Decade ##
    ###################
    plots.display_artist_decades(
        app_config=app_config, 
        ncols=ncols,
        metric_type_option=metric_type_option,
        mapped_metric_type=mapped_metric_type,
    )
    

####################
## Track Duration ##
####################
plots.display_track_duration(
    app_config=app_config,
    ncols=ncols,
    metric_type_option=metric_type_option,
    mapped_metric_type=mapped_metric_type,
)


################
## Top Genres ##
################
plots.display_top_genres(
    app_config=app_config,
    ncols=ncols,
    metric_type_option=metric_type_option,
    mapped_metric_type=mapped_metric_type,
)


### Sentiment Analysis
plots.display_sentiment_analysis(
    app_config=app_config,
    ncols=ncols,
    global_max_mean_values=global_max_mean_values,
)


        

## Visuals
# Create logos for radios of the same size
# Improve visual by trying to add some borders or background colors
# Color PT bar differently
# Define color pallete for each radio


# Allow to select year of release for comparison
# Add filter to select radios
# Make all bar charts the same y-axis
# Add a 'Segmented Control' to allow to choose between seeing percentages or total values (or toggle?)
# Format Weekday names shorter