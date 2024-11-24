import polars as pl
import streamlit as st
import plotly.express as px

from data_extract.data_storage import DataStorage
from data_extract.config_manager import ConfigManager

ds = DataStorage()
cm = ConfigManager()

@st.cache_data
def load_data(path, schema = None):
    data = ds.read_csv(path, schema)
    return data

def calculate_avg_tracks(df: pl.DataFrame) -> float:
    total_tracks = df.height
    unique_days = df.select(pl.col(cm.DAY_COLUMN).n_unique())[0, 0]
    return round(total_tracks / unique_days, 2)

def calculate_avg_hours(df: pl.DataFrame, output_unit: str = "hours") -> float:
    total_duration_ms = df.select(pl.col('spotify_duration_ms').sum())[0, 0]
    unique_days = df.select(pl.col(cm.DAY_COLUMN).n_unique())[0, 0]
    avg_duration_ms = total_duration_ms / unique_days

    # Convert into the desired duration
    if output_unit == 'seconds':
        return round(avg_duration_ms / 1000, 2)
    elif output_unit == 'minutes':
        return round(avg_duration_ms / (1000 * 60), 2)
    elif output_unit == 'hours':
        return round(avg_duration_ms / (1000 * 60 * 60), 2)
    else:
        return round(avg_duration_ms, 2)
    
def calculate_avg_popularity(df: pl.DataFrame) -> float:
    avg = df.select(pl.col('spotify_popularity').mean())[0, 0]
    return round(avg, 2)


import polars as pl
import plotly.express as px

def prepare_hourly_metrics(df: pl.DataFrame, metric: str) -> pl.DataFrame:
    """
    Prepares hourly metrics for plotting.

    Args:
        df: Polars DataFrame with 'time' and the relevant columns.
        metric: One of 'avg_tracks', 'avg_hours_played', or 'avg_popularity'.

    Returns:
        A Polars DataFrame with 'hour' and the calculated metric.
    """
    # Extract the hour from the 'time' column
    df = df.with_columns(
            pl.col(cm.TIME_PLAYED_COLUMN)
            .dt.hour()  # Extract the hour component
            .alias("hour")
        )
    
    # Calculate the chosen metric
    if metric == "avg_tracks":
        hourly_data = (
            df.group_by("hour")
            .agg(pl.col(cm.DAY_COLUMN).n_unique().alias("unique_days"))
            .with_columns(
                (df.height / pl.col("unique_days")).alias("avg_tracks")
            )
        )
    elif metric == "avg_hours_played":
        hourly_data = (
            df.group_by("hour")
            .agg(
                pl.col("spotify_duration_ms").sum().alias("total_duration_ms"),
                pl.col(cm.DAY_COLUMN).n_unique().alias("unique_days")
            )
            .with_columns(
                (pl.col("total_duration_ms") / pl.col("unique_days") / (1000 * 60 * 60)).alias("avg_hours_played")
            )
        )
    elif metric == "avg_popularity":
        hourly_data = (
            df.group_by("hour")
            .agg(pl.col("spotify_popularity").mean().alias("avg_popularity"))
        )
    else:
        raise ValueError(f"Invalid metric: {metric}")
    
    # Return the selected metric
    return hourly_data.select(["hour", metric])


def plot_hourly_metrics(df: pl.DataFrame, metric: str):
    """
    Plots hourly metrics using Plotly.

    Args:
        df: Polars DataFrame with 'hour' and the chosen metric.
        metric: The metric to plot (column name).
    """
    # Convert to a Pandas DataFrame for Plotly compatibility
    pandas_df = df.to_pandas()

    # Create a Plotly column chart
    fig = px.bar(
        pandas_df,
        x="hour",
        y=metric,
        title=f"Hourly {metric.replace('_', ' ').capitalize()}",
        labels={"hour": "Hour of Day", metric: metric.replace('_', ' ').capitalize()},
        text_auto=True
    )
    fig.update_layout(
        xaxis=dict(tickmode="linear", dtick=1),  # Ensure all hours are shown
        yaxis_title=metric.replace('_', ' ').capitalize(),
        xaxis_title="Hour of Day",
    )
    fig.show()




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

# st.write(df)

app_config = cm.load_json(path='dashboard/app_config.json')
# st.write(app_config)

ncols = len(app_config)
cols = st.columns(ncols)
# st.image('dashboard/logo/RadioComercial_logo.jpg')
for i, (key, val) in enumerate(app_config.items()):
    with cols[i]:
        radio_name = val.get('name')
        logo = val.get('logo')
        st.image(logo, use_container_width=True)
        # st.title(radio_name)

        radio_df = df_joined.filter(
            pl.col(cm.RADIO_COLUMN) == radio_name
        )
        
        kpi_1, kpi_2, kpi_3 = st.columns(3)
        kpi_1.metric(
            label='# Avg Tracks per Day',
            value=calculate_avg_tracks(df=radio_df)
        )
        kpi_2.metric(
            label='Avg Hours Played',
            value=calculate_avg_hours(df=radio_df, output_unit='hours')
        )
        kpi_3.metric(
            label='Avg Popularity',
            value=calculate_avg_popularity(df=radio_df)
        )

        hourly_data = prepare_hourly_metrics(radio_df, metric='avg_tracks')
        st.write(hourly_data)
        # st.write(plot_hourly_metrics(hourly_data, metric='avg_tracks'))
        st.write(radio_df)
        



# st.title(radio_name)



