import polars as pl
import streamlit as st
import plotly.express as px
from datetime import timedelta

from data_extract.config_manager import ConfigManager

cm = ConfigManager()

def display_sparkline(radio_df: pl.DataFrame, view_option: str):
    """Displays a sparkline with top X and date-range filters."""

    # 1. Place everything in an expander
    with st.expander("Trend of Plays Over Time", expanded=True):
        with st.popover(label='Settings', icon='⚙️', use_container_width=False):
            # ----------------------------------------------------------------------
            # Toggle for cumulative vs non-cumulative
            # ----------------------------------------------------------------------
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

        # Limit default visibility to top 5 entities
        default_visible_entities = sorted_top_entities.head(5)['display_label' if view_option == 'Track' else cm.ARTIST_NAME_COLUMN].to_list()

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
        fig.update_layout(height=600)

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