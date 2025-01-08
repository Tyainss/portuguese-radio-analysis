import polars as pl
import streamlit as st
import plotly.express as px
from datetime import timedelta

from data_extract.config_manager import ConfigManager

cm = ConfigManager()

def display_sparkline(radio_df: pl.DataFrame, view_option: str):
    """Displays a sparkline with top X and date-range filters."""

    with st.expander("Trend of Plays Over Time", expanded=True):
        # Handle empty dataframe scenario
        if radio_df.is_empty():
            st.warning("No available data to display.")
            return
        with st.popover(label='Settings', icon='⚙️', use_container_width=False):
            # Toggle for cumulative vs non-cumulative
            cumulative_toggle = st.toggle(
                "Cumulative View",
                value=False, 
                help="Check to see a cumulative sum of plays over time."
            )
            # Add a filter to select top X
            top_x = st.number_input(
                "Number of top items", 
                min_value=1, 
                max_value=50, 
                value=5, 
                step=1,
                help="Select how many top artists/tracks to display."
            )

            # Add a filter to select the range of dates
            #    By default last 30 days, but user can expand
            min_date_in_data = radio_df[cm.DAY_COLUMN].min()
            max_date_in_data = radio_df[cm.DAY_COLUMN].max()

            # Ensure valid min/max dates before using timedelta
            if min_date_in_data is None or max_date_in_data is None:
                st.warning("No date data available.")
                return

            default_end = max_date_in_data
            default_start = max(min_date_in_data, max_date_in_data - timedelta(days=30))

            date_range = st.date_input(
                label="Select date range",
                value=(default_start, default_end),
                min_value=min_date_in_data,
                max_value=max_date_in_data,
                help="Pick a date range to display. \n\nIt will combine it with the time period defined on Page Settings"
            )

        # Unpack the user-chosen range
        start_date, end_date = date_range if len(date_range) == 2 else (default_start, default_end)        

        # Data transformations: Filter the main DataFrame by user-selected date range
        df_filtered = radio_df.filter(
            (pl.col(cm.DAY_COLUMN) >= start_date) & (pl.col(cm.DAY_COLUMN) <= end_date)
        )

        if view_option == "Artist":
            group_cols = [cm.ARTIST_NAME_COLUMN]
        else:  # "Track"
            group_cols = [cm.ARTIST_NAME_COLUMN, cm.TRACK_TITLE_COLUMN]

        # Aggregate daily plays (each row is a play)
        plays_by_day = (
            df_filtered
            .group_by(group_cols + [cm.DAY_COLUMN])
            .agg(pl.count().alias('play_count'))
        )

        # Ensure all days are covered (fill missing dates with 0 plays)
        all_dates = pl.DataFrame({cm.DAY_COLUMN: pl.date_range(
            start=df_filtered[cm.DAY_COLUMN].min(),
            end=df_filtered[cm.DAY_COLUMN].max(),
            interval='1d',
            eager=True
        )})

        distinct_entities = plays_by_day.select(group_cols).unique()
        all_combinations = distinct_entities.join(all_dates, how='cross')

        filled_data = (
            all_combinations
            .join(plays_by_day, on=group_cols + [cm.DAY_COLUMN], how='left')
            .with_columns(pl.col('play_count').fill_null(0))
        )

        # Compute cumulative sum if toggle is enabled
        if cumulative_toggle:
            filled_data = (
                filled_data.sort(group_cols + [cm.DAY_COLUMN])
                .with_columns(pl.col('play_count').cum_sum().over(group_cols).alias('cumulative_play_count'))
            )
            value_col = 'cumulative_play_count'
        else:
            value_col = 'play_count'

        # Decide top X by total plays in the selected date range
        sorted_top_entities = (
            filled_data.group_by(group_cols)
            .agg(pl.col(value_col).sum().alias("total_plays"))
            .sort("total_plays", descending=True)
            .head(top_x)
        )

        # Filter main data to only top X entities
        top_data = filled_data.join(sorted_top_entities, on=group_cols, how="inner")

        # Ensure fixed sorting order for display labels
        if view_option == 'Track':
            sorted_top_entities = sorted_top_entities.with_columns(
                (pl.col(cm.TRACK_TITLE_COLUMN) + ' - ' + pl.col(cm.ARTIST_NAME_COLUMN)).alias('display_label')
            )
            top_data = top_data.with_columns(
                (pl.col(cm.TRACK_TITLE_COLUMN) + ' - ' + pl.col(cm.ARTIST_NAME_COLUMN)).alias('display_label')
            )
            color_col = 'display_label'
        else:
            color_col = cm.ARTIST_NAME_COLUMN

        # Fix ordering for consistent colors
        top_data = top_data.join(sorted_top_entities.select(group_cols), on=group_cols, how='left')

        # Plot the sparkline
        fig = px.line(
            top_data.to_pandas(), 
            x=cm.DAY_COLUMN, 
            y=value_col,
            color=color_col,
            category_orders={color_col: sorted_top_entities[color_col].to_list()},
            title=f'Trend of {view_option} Plays Over Time',
            labels={value_col: 'Plays', cm.DAY_COLUMN: 'Date'},
            template='plotly_white',
            line_shape="spline",
        )
        # Determine the min and max values for the y-axis
        y_max = top_data[value_col].max()
        fig.update_yaxes(range=[0, y_max * 1.1])  # Add some padding (10%) for better visibility

        # Increase height
        fig.update_layout(
                xaxis_title=None,
                yaxis_title=None,
                margin=dict(l=10, r=30, t=30, b=0),
                height=600,
                hoverlabel_align="left",
            )

        # Define line width based on total plays
        line_widths = {label: 2 + (total_plays / sorted_top_entities['total_plays'].max()) * 1.5
                    for label, total_plays in zip(sorted_top_entities[color_col], sorted_top_entities['total_plays'])}

        for trace in fig.data:
            trace.line.width = line_widths.get(trace.name, 2)  # Adjust width dynamically

        st.write(
            "**Tip**: You can hover over the lines to see exact values. "
            "You can also click legend entries to toggle them on/off."
        )

        st.plotly_chart(fig, use_container_width=True)


def display_plot_dataframe(radio_df: pl.DataFrame, view_option: str):
    st.subheader("60-Day Overview")
    # Handle empty dataframe scenario
    if radio_df.is_empty():
        st.warning("No available data to display.")
        return
    
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

    # Identify last 60 days from the max date for the sparkline
    max_date_in_df = df_all_time[cm.DAY_COLUMN].max()
    last_60_days_start = max_date_in_df - timedelta(days=61)
    last_60_days_end   = max_date_in_df - timedelta(days=1)
    df_60_days = radio_df.filter(
        (pl.col(cm.DAY_COLUMN) >= last_60_days_start)
        & (pl.col(cm.DAY_COLUMN) <= last_60_days_end)
    )

    # Build the date range for zero-filling
    date_series = pl.date_range(
        start=last_60_days_start,
        end=last_60_days_end,
        interval="1d",
        eager=True  # returns a Polars Series directly
    )
    all_dates = pl.DataFrame({cm.DAY_COLUMN: date_series})

    # Cross-join all dates with dimension combos
    dim_combos = df_60_days.select(group_cols).unique()
    all_combinations = dim_combos.join(all_dates, how='cross')

    # Count daily plays within last 60 days
    daily_counts_60 = (
        df_60_days
        .group_by(group_cols + [cm.DAY_COLUMN])
        .agg([pl.count().alias('plays_per_day')])
    )

    # Zero-fill missing dates for the sparkline
    zero_filled = (
        all_combinations
        .join(daily_counts_60, on=group_cols + [cm.DAY_COLUMN], how='left')
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
        label="Daily Plays (Last 60d)",
        width="big",
        help="Zero-filled daily plays for last 60 days"
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

    st.data_editor(
        df_to_display,
        column_config=col_config,
        hide_index=True,
        use_container_width=True
    )