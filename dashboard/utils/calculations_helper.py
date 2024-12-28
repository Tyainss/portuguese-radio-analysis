import streamlit as st
import polars as pl
import plotly.express as px
from typing import List

from data_extract.data_storage import DataStorage
from data_extract.config_manager import ConfigManager

ds = DataStorage()
cm = ConfigManager()

def _convert_ms(duration_ms, output_unit: str = 'hours') -> float:
    # Convert into the desired duration
    conversion_factors = {
        'milliseconds': 1,
        'seconds': 1 / 1000,
        'minutes': 1 / (1000 * 60),
        'hours': 1 / (1000 * 60 * 60)
    }
    factor = conversion_factors.get(output_unit, 1)
    return round(duration_ms * factor, 2)

def prepare_hourly_metrics(df: pl.DataFrame, metric: str, **kwargs) -> pl.DataFrame:
    """
    Prepares hourly metrics for plotting.

    Args:
        df: Polars DataFrame with 'time' and the relevant columns.
        metric: One of 'avg_tracks', 'avg_time_played', or 'avg_popularity'.

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
            .agg(
                pl.col(cm.DAY_COLUMN).n_unique().alias("unique_days"),
                pl.col(cm.DAY_COLUMN).count().alias("tracks"),
            )
            .with_columns(
                (pl.col("tracks") / pl.col('unique_days')).alias("avg_tracks"),
            )
        )
    elif metric == "avg_time_played":
        output_unit = kwargs.get('output_unit', 'hours')
        conversion_factor = {
            'milliseconds': 1,
            'seconds': 1 / 1000,
            'minutes': 1 / (1000 * 60),
            'hours': 1 / (1000 * 60 * 60)
        }.get(output_unit, 1)

        hourly_data = (
            df.group_by("hour")
            .agg(
                pl.col("spotify_duration_ms").sum().alias("total_duration_ms"),
                pl.col(cm.DAY_COLUMN).n_unique().alias("unique_days")
            )
            .with_columns(
                ((pl.col("total_duration_ms") / pl.col("unique_days")) * conversion_factor)
                .alias("avg_time_played")
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
    result = hourly_data.select(["hour", metric]).sort('hour', descending=False)
    # result = hourly_data
    return result

def prepare_weekday_metrics(df: pl.DataFrame, metric: str, output_unit: str = 'hours') -> pl.DataFrame:
    """
    Prepares weekday metrics for plotting.

    Args:
        df: Polars DataFrame with 'day' and the relevant columns.
        metric: One of 'avg_tracks', 'avg_time_played', or 'avg_popularity'.

    Returns:
        A Polars DataFrame with 'weekday' and the calculated metric.
    """
    # Extract the weekday from the 'day' column
    df = df.with_columns(
        pl.col(cm.DAY_COLUMN).dt.to_string('%A').alias("weekday"),
        pl.col(cm.DAY_COLUMN).dt.weekday().alias("weekday_number"),
    )

    # Calculate the chosen metric
    if metric == "avg_tracks":
        weekday_data = (
            df.group_by("weekday", "weekday_number")
            .agg(
                pl.col(cm.DAY_COLUMN).n_unique().alias("unique_days"),
                pl.count().alias("tracks"),
            )
            .with_columns(
                (pl.col("tracks") / pl.col("unique_days")).alias("avg_tracks")
            )
        )
    elif metric == "avg_time_played":
        conversion_factor = {
            'milliseconds': 1,
            'seconds': 1 / 1000,
            'minutes': 1 / (1000 * 60),
            'hours': 1 / (1000 * 60 * 60)
        }.get(output_unit, 1)
        weekday_data = (
            df.group_by("weekday", "weekday_number")
            .agg(
                pl.col("spotify_duration_ms").sum().alias("total_duration_ms"),
                pl.col(cm.DAY_COLUMN).n_unique().alias("unique_days")
            )
            .with_columns(
                ((pl.col("total_duration_ms") / pl.col("unique_days")) * conversion_factor)
                .alias("avg_time_played")
            )
        )
    elif metric == "avg_popularity":
        weekday_data = (
            df.group_by("weekday", "weekday_number")
            .agg(pl.col("spotify_popularity").mean().alias("avg_popularity"))
        )
    else:
        raise ValueError(f"Invalid metric: {metric}")

    # Sort by weekday
    weekday_data = weekday_data.sort("weekday_number")

    # Map weekday numbers to names
    weekday_data = weekday_data.with_columns(
        pl.col("weekday").alias("weekday_name")
    )

    return weekday_data.select(["weekday_name", metric])

def calculate_avg_tracks(df: pl.DataFrame, adjusted_calc=True) -> float:
    if df.is_empty():
        return 0.0
    if not adjusted_calc:
        total_tracks = df.height
        unique_days = df.select(pl.col(cm.DAY_COLUMN).n_unique())[0, 0]
        avg_tracks = total_tracks / unique_days
    else:
        hourly_data = prepare_hourly_metrics(df, metric='avg_tracks')
        avg_tracks = hourly_data.select(pl.col('avg_tracks').sum())[0,0]

    return round(avg_tracks, 2)

def calculate_avg_time(df: pl.DataFrame, output_unit: str = "hours", adjusted_calc=True) -> float:
    if df.is_empty():
        return 0.0
    if not adjusted_calc:
        total_duration_ms = df.select(pl.col('spotify_duration_ms').sum())[0, 0]
        unique_days = df.select(pl.col(cm.DAY_COLUMN).n_unique())[0, 0]
        avg_duration_ms = total_duration_ms / unique_days
        avg_time = _convert_ms(avg_duration_ms, output_unit=output_unit)
    else:
        hourly_data = prepare_hourly_metrics(df, metric='avg_time_played', kwargs=output_unit)
        avg_time = hourly_data.select(pl.col('avg_time_played').sum())[0, 0]

    return round(avg_time, 2)
    
    
def calculate_avg_popularity(df: pl.DataFrame) -> float:
    if df.is_empty():
        return 0.0
    avg = df.select(pl.col('spotify_popularity').mean())[0, 0]
    return round(avg, 2)


def plot_metrics(
    df: pl.DataFrame, 
    metric: str, 
    radio_name: str, 
    x_axis_column: str = 'weekday_name',
    x_axis_label: str = 'Day of Week',
    y_axis_range: tuple[float, float] = None,
    **kwargs
):
    """
    Plots metrics using Plotly for either weekday or hourly data.

    Args:
        df: Polars DataFrame with the chosen x-axis column and metric.
        metric: The metric to plot (column name).
        radio_name: Name of the radio station for the plot title.
        x_axis_column: Column name for the x-axis ('weekday_name' or 'hour').
        x_axis_label: Label for the x-axis.
        y_axis_range: Optional tuple specifying (min, max) for the y-axis range.
    """
    # Convert to a Pandas DataFrame for Plotly compatibility
    pandas_df = df.to_pandas()

    title = kwargs.get('title', f"{radio_name} - {metric.replace('_', ' ').capitalize()} by {x_axis_label}")

    # Create a Plotly line chart
    fig = px.line(
        pandas_df,
        x=x_axis_column,
        y=metric,
        title=title,
        labels={x_axis_column: x_axis_label, metric: metric.replace('_', ' ').capitalize()},
        markers=True
    )
    fig.update_layout(
        xaxis_title=x_axis_label,
        yaxis_title=metric.replace('_', ' ').capitalize(),
    )

    # Apply the y-axis range if provided
    if y_axis_range:
        fig.update_yaxes(range=y_axis_range)

    st.plotly_chart(fig, use_container_width=True, key=f'{radio_name}_{metric}_{x_axis_column}')


def calculate_column_counts(
    df: pl.DataFrame,
    group_by_cols: str,
    count_columns: List[str],
    metric_type: str = 'unique'
) -> pl.DataFrame:
    """
    Calculate counts or averages for a given column, grouped by specified columns, based on the selected metric type.

    Args:
        df (pl.DataFrame): Input DataFrame containing the data.
        group_by_cols (str): Column to group by.
        count_columns (List[str]): Columns to count occurrences of.
        metric_type (str, optional): Metric type ('unique', 'total', or 'average'). Defaults to 'unique'.

    Returns:
        pl.DataFrame: Grouped DataFrame with counts or averages and a 'metric' column.
    """
    cols = [pl.col(col) for col in count_columns] + [pl.col(group_by_cols)]

    # Calculate metrics based on the selected type
    if metric_type == "unique":
        counts = df.select(cols).unique()
        result = (
            counts
            .group_by(group_by_cols)
            .count()
            .rename({"count": "metric"})
        )
    elif metric_type == "total":
        counts = df.select(cols)
        result = (
            counts
            .group_by(group_by_cols)
            .count()
            .rename({"count": "metric"})
        )
    elif metric_type == "average":
        unique_counts = (
            df.select(cols).unique()
            .group_by(group_by_cols)
            .count()
            .rename({"count": "unique_count"})
        )
        total_counts = (
            df.select(cols)
            .group_by(group_by_cols)
            .count()
            .rename({"count": "total_count"})
        )
        result = unique_counts.join(total_counts, on=group_by_cols)
        result = result.with_columns(
            (pl.col("total_count") / pl.col("unique_count")).alias("metric")
        ).select([group_by_cols, "metric"])
    else:
        raise ValueError(f"Unsupported metric_type: {metric_type}. Choose 'unique', 'total', or 'average'.")

    return result.sort(by="metric", descending=True)
