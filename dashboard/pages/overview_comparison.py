import polars as pl
import streamlit as st
import plotly.express as px

from data_extract.data_storage import DataStorage
from data_extract.config_manager import ConfigManager

from utils.helper import (
    calculate_avg_tracks, calculate_avg_popularity, calculate_avg_time,
    prepare_weekday_metrics, prepare_hourly_metrics, plot_metrics
)

ds = DataStorage()
cm = ConfigManager()
app_config = cm.load_json(path='dashboard/app_config.json')

@st.cache_data
def load_data(path, schema = None):
    data = ds.read_csv(path, schema)
    return data


# Load the data
df_radio_data = load_data(cm.RADIO_CSV_PATH, cm.RADIO_SCRAPPER_SCHEMA)
df_artist_info = load_data(cm.ARTIST_INFO_CSV_PATH, cm.ARTIST_INFO_SCHEMA)
df_track_info = load_data(cm.TRACK_INFO_CSV_PATH, cm.TRACK_INFO_SCHEMA)

df_joined = df_radio_data.join(
        df_artist_info, 
        on=cm.ARTIST_NAME_COLUMN,
        how='left',
    ).join(
        df_track_info,
        on=[cm.TRACK_TITLE_COLUMN, cm.ARTIST_NAME_COLUMN],
        how='left',
    )

# Filter df for data validation checks
# from datetime import datetime
# df_joined = df_joined.filter(
#     pl.col('day') >= datetime(2024, 12, 14),
#     pl.col('day') <= datetime(2024, 12, 20),
# )

# st.write(df)

# Calculate global min and max values
metrics = ['avg_tracks', 'avg_time_played', 'avg_popularity']
metric_ranges = {}
# Initialize config for each radio
for i, (key, val) in enumerate(app_config.items()):
    app_config[key]['radio_df'] = df_joined.filter(
            pl.col(cm.RADIO_COLUMN) == val.get('name')
        )
    for metric in metrics:
        weekday_metric_df = prepare_weekday_metrics(app_config[key]['radio_df'], metric=metric)
        hour_metric_df = prepare_hourly_metrics(app_config[key]['radio_df'], metric=metric)
        if metric not in metric_ranges:
            metric_ranges[metric] = {
                'weekday': {'min': float('inf'), 'max': float('-inf')},
                'hour': {'min': float('inf'), 'max': float('-inf')},
            }
        # Update weekday min/max
        metric_ranges[metric]['weekday']['min'] = min(metric_ranges[metric]['weekday']['min'], weekday_metric_df[metric].min())
        metric_ranges[metric]['weekday']['max'] = max(metric_ranges[metric]['weekday']['max'], weekday_metric_df[metric].max())
        # Update hour min/max
        metric_ranges[metric]['hour']['min'] = min(metric_ranges[metric]['hour']['min'], hour_metric_df[metric].min())
        metric_ranges[metric]['hour']['max'] = max(metric_ranges[metric]['hour']['max'], hour_metric_df[metric].max())

# st.write(metric_ranges)

# Sidebar Settings
with st.sidebar:
    st.title(':gear: Page Settings')
    graph_otion = st.radio(
        'Select Graph to Display:',
        ['Avg Tracks', 'Avg Hours Played', 'Avg Popularity'],
        index=0
    )

graph_metric_map = {
    'Avg Tracks': 'avg_tracks',
    'Avg Hours Played': 'avg_time_played',
    'Avg Popularity': 'avg_popularity'
}
selected_metric = graph_metric_map[graph_otion]

# Main Layout
ncols = len(app_config)
cols = st.columns(ncols)

for i, (key, val) in enumerate(app_config.items()):
    with cols[i]:
        radio_name = val.get('name')
        logo = val.get('logo')
        radio_df = val.get('radio_df')
        st.image(logo, use_container_width=True)
        # st.title(radio_name)

        
        kpi_1, kpi_2, kpi_3 = st.columns(3)
        kpi_1.metric(
            label='# Avg Tracks per Day',
            value=calculate_avg_tracks(df=radio_df)
        )
        kpi_2.metric(
            label='Avg Hours Played',
            value=calculate_avg_time(df=radio_df, output_unit='hours')
        )
        kpi_3.metric(
            label='Avg Popularity',
            value=calculate_avg_popularity(df=radio_df)
        )

        # Prepare the selected Weekday Metric
        weekday_df = prepare_weekday_metrics(radio_df, metric=selected_metric)
        hourly_df = prepare_hourly_metrics(radio_df, metric=selected_metric)
        st.subheader(f'{graph_otion}')
        plot_metrics(
            weekday_df,
            metric=selected_metric,
            radio_name=radio_name,
            x_axis_column='weekday_name',
            x_axis_label='Day of Week',
            y_axis_range=(
                metric_ranges[selected_metric]['weekday']['min'] * 0.99,
                metric_ranges[selected_metric]['weekday']['max'] * 1.01
            ),
            title=''
        )
        plot_metrics(
            hourly_df,
            metric=selected_metric,
            radio_name=radio_name,
            x_axis_column='hour',
            x_axis_label='Hour of Day',
            y_axis_range=(
                metric_ranges[selected_metric]['hour']['min'] * 0.95,
                metric_ranges[selected_metric]['hour']['max'] * 1.05
            ),
            title=''
        )
        

        st.write(radio_df)
        

# Perhaps make Avg Hours as the first/main graph