import polars as pl
import pandas as pd
import streamlit as st
from streamlit_extras.stylable_container import stylable_container
import plotly.express as px
import plotly.graph_objects as go

from utils import calculations, helper
from utils.overview_comparison import mappings
# from utils.helper import helper.number_formatter, language_full_name_dict

from data_extract.config_manager import ConfigManager
cm = ConfigManager()

def display_header_kpis(app_config: dict, ncols: int):
    header_cols = st.columns(ncols)
    for i, (_, val) in enumerate(app_config.items()):
        with header_cols[i]:
            radio_name = val.get('name')
            logo = val.get('logo')
            radio_df = val.get('radio_df')
            # st.image(logo, use_container_width=True)
            with st.container(border=True):
                kpi_1, kpi_2, kpi_3 = st.columns(3)
                kpi_1.metric(
                    label='# Avg Daily Tracks',
                    value=calculations.calculate_avg_tracks(_df=radio_df, id=radio_name)
                )
                kpi_2.metric(
                    label='Avg Daily Hours',
                    value=calculations.calculate_avg_time(_df=radio_df, output_unit='hours', id=radio_name)
                )
                kpi_3.metric(
                    label='Avg Popularity',
                    value=calculations.calculate_avg_popularity(_df=radio_df, id=radio_name),
                    help='''Popularity is a **score** that reflects **how frequently a track has been played, 
                    saved, or added to playlists** by users on :green[Spotify], with recent activity weighing more heavily than older interactions.'''
                )


def display_hourly_graph(app_config: dict, ncols: int, selected_metric: str, metric_ranges: dict):
    st.subheader(
        f'{st.session_state['ts_graph']} :blue[by hour]', 
        divider="gray",
        help="Track duration extracted from :green[Spotify].\n\nThe average played time of an hour can be above 1 if the song is not played in its entirety on the radio."
    )
    hour_graph_cols = st.columns(ncols)
    for i, (_, val) in enumerate(app_config.items()):
        with hour_graph_cols[i]:
            radio_name = val.get('name')
            radio_df = val.get('radio_df')
            hourly_df = calculations.prepare_hourly_metrics(radio_df, metric=selected_metric, id=radio_name)
            calculations.plot_metrics(
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

def display_weekly_graph(app_config: dict, ncols: int, selected_metric: str, metric_ranges: dict):
    st.subheader(f'{st.session_state['ts_graph']} :blue[by weekday]', divider="gray")
    weekday_graph_cols = st.columns(ncols)

    for i, (_, val) in enumerate(app_config.items()):
        with weekday_graph_cols[i]:
            radio_name = val.get('name')
            radio_df = val.get('radio_df')
            # Prepare the selected Weekday Metric
            weekday_df = calculations.prepare_weekday_metrics(radio_df, metric=selected_metric, id=radio_name)
            calculations.plot_metrics(
                weekday_df,
                metric=selected_metric,
                radio_name=radio_name,
                x_axis_column='weekday_name',
                x_axis_label='Day of Week',
                y_axis_range=(
                    metric_ranges[selected_metric]['weekday']['min'] * 0.95,
                    metric_ranges[selected_metric]['weekday']['max'] * 1.05
                ),
                title=''
            )

def display_track_kpis(app_config: dict, ncols: int):
    st.header(f':musical_note: Track Statistics', divider="gray")
    track_kpis_cols = st.columns(ncols)

    for i, (_, val) in enumerate(app_config.items()):
        with track_kpis_cols[i]:
            radio_df = val.get('radio_df')

            total_tracks = radio_df.height
    
            # Track KPIs
            track_kpi_total, track_kpi_percent = st.columns(2, border=True)
            with track_kpi_total:
                st.metric(
                    label='Total Tracks',
                    value=helper.number_formatter(total_tracks)
                )
                unique_tracks = radio_df.select([pl.col(cm.TRACK_TITLE_COLUMN), pl.col(cm.ARTIST_NAME_COLUMN)]).unique().height

            percent_unique_tracks = f'{(unique_tracks / total_tracks * 100):.2f}%' if total_tracks > 0 else "N/A"
            with track_kpi_percent:
                st.metric(
                    label='% Unique Tracks',
                    value=percent_unique_tracks
                )
            
            avg_plays_per_track = f'{(total_tracks / unique_tracks if unique_tracks > 0 else 0):.2f}'
            st.write(f'That means each track is played **{avg_plays_per_track}** times on average')


def display_track_languages(app_config: dict, ncols: int, num_languages: int, mapped_metric_type: str):
    st.subheader(
        f':earth_africa: :blue[Top {num_languages} Languages] by {st.session_state['metric_type']} Tracks', 
        divider="gray",
        help='Language detected by :blue-background[**analysing the lyrics**] with a language-detection Python library'
    )
    track_plots_cols = st.columns(ncols)

    for i, (_, val) in enumerate(app_config.items()):
        with track_plots_cols[i]:
            radio_name = val.get('name')
            radio_df = val.get('radio_df')
            
            # st.write(radio_df)
            language_counts = calculations.calculate_country_counts(
                _df=radio_df,
                country_col='lyrics_language',
                count_columns=[cm.TRACK_TITLE_COLUMN, cm.ARTIST_NAME_COLUMN],
                metric_type=mapped_metric_type,
                include_most_played="track",
                id=radio_name,
            )

            # Separate top languages and "Others"
            top_languages = language_counts.head(num_languages)
            others = language_counts.tail(language_counts.shape[0] - min(num_languages, language_counts.shape[0]))

            if not others.is_empty():
                # Use values from the first row of 'others' for the top artist and play count
                most_played_track = others[0, "most_played_track"]
                most_played_artist = others[0, "most_played_artist"]
                most_played_count = others[0, "most_played_count"]
            else:
                most_played_track = None
                most_played_artist = None
                most_played_count = None

            if mapped_metric_type == "average":
                others_aggregated = pl.DataFrame({
                    "lyrics_language": ["Others"],
                    "metric": [others["metric"].mean()],
                    "most_played_track": [most_played_track],
                    "most_played_artist": [most_played_artist],
                    "most_played_count": [most_played_count]
                })
            else:
                others_aggregated = pl.DataFrame({
                    "lyrics_language": ["Others"],
                    "metric": [others["metric"].sum()],
                    "most_played_track": [most_played_track],
                    "most_played_artist": [most_played_artist],
                    "most_played_count": [most_played_count]
                })

            # Ensure all columns match between top_languages and others_aggregated
            for column in top_languages.columns:
                if column not in others_aggregated.columns:
                    others_aggregated = others_aggregated.with_columns(pl.lit(None).cast(top_languages[column].dtype).alias(column))
                else:
                    others_aggregated = others_aggregated.with_columns(pl.col(column).cast(top_languages[column].dtype))

            all_languages = top_languages.vstack(others_aggregated)

            # Calculate percentage only for 'Unique Tracks' or 'Total Tracks'
            if mapped_metric_type in ["unique", "total"]:
                all_languages = all_languages.with_columns(
                    (pl.col("metric") / all_languages["metric"].sum() * 100).alias("percentage")
                )

            
            # Add flag mapping
            all_languages = all_languages.with_columns(
                pl.col("lyrics_language").replace_strict(helper.language_full_name_dict, default='Others').alias("full_language_name")
            )
            # Add flag mapping
            all_languages = all_languages.with_columns(
                pl.col("lyrics_language").replace_strict(helper.language_to_flag_dict, default='Others').alias("flag")
            )
            
            # Convert to Pandas for Plotly
            data_df = all_languages.to_pandas()

            # Ensure "Others" is always last in the plot
            data_df['order'] = data_df['flag'].apply(lambda x: 1 if x == "Others" else 0)
            
            data_df = data_df.sort_values(by=['order', 'metric'], ascending=[False, True])

            # Add hover text
            data_df["tooltip_text"] = data_df.apply(
                lambda row: (
                    f"<b>Abbreviation:</b> {row['flag']}<br>"
                    f"<b>Language:</b> {row['full_language_name']}<br>"
                    f"<b>{mapped_metric_type.capitalize()}:</b> {helper.number_formatter(row['metric'])}<br>"
                    f"<b>Most Played Track:</b> {row['most_played_track']} | {row['most_played_artist']} "
                    f"({helper.number_formatter(row['most_played_count'])} plays)"
                ),
                axis=1
            )
            
            if mapped_metric_type == "average":
                data_df["label"] = data_df["metric"].apply(lambda x: f"{x:.1f}")
            else:
                data_df["label"] = data_df["percentage"].apply(lambda x: f"{x:.1f}%")

            # Plot top languages
            fig = px.bar(
                data_df,
                x="metric",
                y="flag",
                text="label",
                title="",
                orientation='h',
                hover_data={"tooltip_text": True},
            )

            # Apply conditional coloring for "Portugal" or "PT"
            colors = [
                "#1f77b4" if lang != "PT" else "#ff7f0e"  # Default color vs highlight color
                for lang in data_df["flag"]
            ]
            fig.update_traces(
                marker_color=colors,  # Apply colors manually
                textposition="outside",
                hovertemplate="%{customdata[0]}",
                customdata=data_df[["tooltip_text"]].to_numpy(),
                cliponaxis=False  # Prevent labels from being clipped
            )
            fig.update_layout(
                xaxis_title=None,  # Remove x-axis label
                yaxis_title=None,  # Remove y-axis label
                margin=dict(l=10, r=30, t=30, b=0),  # Add padding around the plot
                height=400,
                hoverlabel_align="left",
            )
            st.plotly_chart(fig, use_container_width=True, key=f'{radio_name}_tracks_by_language')

def display_track_decades(app_config: dict, ncols: int, metric_type_option: str, mapped_metric_type: str):
    st.subheader(
        f':date: {st.session_state['metric_type']} Tracks by :blue[*decade*]', 
        divider="gray", 
        help="""Based on the :blue-background[**year of release**] of the track
        \n\nData Obtained from :green[Spotify]"""
    )
    track_decade_cols = st.columns(ncols)

    for i, (_, val) in enumerate(app_config.items()):
        with track_decade_cols[i]:
            radio_name = val.get('name')
            radio_df = val.get('radio_df')

            # Calculate metrics by decade, including most played track
            df_decades_tracks = calculations.calculate_decade_metrics(
                _df=radio_df,
                date_column='spotify_release_date',
                count_columns=[cm.TRACK_TITLE_COLUMN, cm.ARTIST_NAME_COLUMN],
                metric_type=mapped_metric_type,
                include_most_played="track",
                id=radio_name,
            )

            # Plot unique tracks by decade
            df_tracks_decade_pandas = df_decades_tracks.to_pandas()
            
            # Display the values percentages
            total_tracks = df_tracks_decade_pandas["metric"].sum()
            # df_tracks_decade_pandas["percentage"] = (df_tracks_decade_pandas["metric"] / total_tracks) * 100
            df_tracks_decade_pandas["percentage"] = (
                df_tracks_decade_pandas["metric"] / total_tracks * 100
            ).apply(lambda x: f"{x:.1f}%")

            df_tracks_decade_pandas["formatted_metric"] = df_tracks_decade_pandas["metric"].apply(lambda x: helper.number_formatter(x))

            # Add hover text
            df_tracks_decade_pandas["tooltip_text"] = df_tracks_decade_pandas.apply(
                lambda row: (
                    f"<b>Decade:</b> {row['decade_label']}<br>"
                    f"<b>{metric_type_option} Tracks:</b> {row['formatted_metric']}<br>"
                    f"<b>Percentage:</b> {row['percentage']}<br>"
                    f"<b>Most Played Track:</b> {row['most_played_track']} | {row['most_played_artist']} "
                    f"({helper.number_formatter(row['most_played_count'])} plays)"
                ),
                axis=1
            )

            fig_tracks = px.bar(
                df_tracks_decade_pandas,
                x="decade_label",
                y="metric",
                title="",
                labels={"decade_label": "Decade", "metric": "Tracks"},
                orientation='v',
                text="percentage",  # Display the count on the bars
                hover_data={"tooltip_text": True}
            )
            fig_tracks.update_traces(
                hovertemplate="%{customdata[0]}",
                customdata=df_tracks_decade_pandas[["tooltip_text"]].to_numpy(),
                texttemplate="%{text}",
                textposition="outside"
            )
            fig_tracks.update_layout(
                xaxis_title=None,
                yaxis_title=None,
                margin=dict(l=10, r=30, t=30, b=0),
                height=400,
                xaxis=dict(type='category', categoryorder='array', categoryarray=df_tracks_decade_pandas["decade_label"].tolist()),  # Ensure proper ordering
                hoverlabel_align="left",
            )
            st.plotly_chart(fig_tracks, use_container_width=True, key=f'{radio_name}_unique_tracks_by_decade')

            unique_2024_tracks = radio_df.filter(pl.col('spotify_release_date').dt.year() == 2024).select([pl.col(cm.TRACK_TITLE_COLUMN), pl.col(cm.ARTIST_NAME_COLUMN)]).unique().height
            total_2024_tracks = radio_df.filter(pl.col('spotify_release_date').dt.year() == 2024).height
            # Avoid division by zero
            if unique_2024_tracks > 0:
                average_plays_per_track = total_2024_tracks / unique_2024_tracks
            else:
                average_plays_per_track = 0.0
            st.markdown(
                f'''**{unique_2024_tracks}** unique tracks released in :blue-background[2024], and were played a total of **{helper.number_formatter(total_2024_tracks)}** times
                \ni.e. **{average_plays_per_track:.1f}** times per track'''
            )

def display_artist_kpis(app_config: dict, ncols: int):
    st.header(f':microphone: Artist Statistics', divider="gray")
    artist_kpis_cols = st.columns(ncols)

    for i, (_, val) in enumerate(app_config.items()):
        with artist_kpis_cols[i]:
            radio_df = val.get('radio_df')
            total_tracks = radio_df.height

            # Artists KPIs
            artist_kpi_total, artist_kpi_percent = st.columns(2, border=True)
            with artist_kpi_total:
                unique_artists = radio_df.select(pl.col(cm.ARTIST_NAME_COLUMN)).unique().height
                st.metric(
                    label='Unique Artists',
                    value=helper.number_formatter(unique_artists)
                )
                
            with artist_kpi_percent:
                avg_plays_per_artist = f'{(total_tracks / unique_artists if unique_artists > 0 else 0):.2f}'
                st.metric(
                    label='Avg Tracks per Artist',
                    value=avg_plays_per_artist
                )

def display_artist_countries(app_config: dict, ncols: int, num_countries: int, mapped_metric_type: str):
    st.subheader(
        f':earth_africa: :blue[Top {num_countries} countries] by {st.session_state['metric_type']} Artists', 
        divider="gray",
        help='Country data extracted from :blue-background[**MusicBrainz**] and complemented with :blue-background[**Wikipedia**] when missing.'
        )
    artist_plots_cols = st.columns(ncols)

    for i, (_, val) in enumerate(app_config.items()):
        with artist_plots_cols[i]:
            radio_name = val.get('name')
            radio_df = val.get('radio_df')

            # Add flag mapping
            radio_df = radio_df.with_columns(
                pl.col("combined_nationality").replace_strict(helper.nationality_to_flag_dict, default='?').alias("flag")
            )
            
            country_counts = calculations.calculate_country_counts(
                _df=radio_df,
                country_col='flag',
                count_columns=[cm.ARTIST_NAME_COLUMN],
                metric_type=mapped_metric_type,
                include_most_played="artist",
                id=radio_name,
            )
            
            # Separate top countries and "Others"
            top_countries = country_counts.head(num_countries)
            others = country_counts.tail(country_counts.shape[0] - min(num_countries, country_counts.shape[0]))
            
            if not others.is_empty():
                # Use values from the first row of 'others' for the top artist and play count
                most_played_artist = others[0, "most_played_artist"]
                most_played_count = others[0, "most_played_count"]
            else:
                most_played_artist = None
                most_played_count = None

            if mapped_metric_type == "average":
                others_aggregated = pl.DataFrame({
                    "flag": ["Others"],
                    "metric": [others["metric"].mean()],
                    "most_played_artist": [most_played_artist],
                    "most_played_count": [most_played_count]
                })
            else:
                others_aggregated = pl.DataFrame({
                    "flag": ["Others"],
                    "metric": [others["metric"].sum()],
                    "most_played_artist": [most_played_artist],
                    "most_played_count": [most_played_count]
                })

            # Ensure 'others_aggregated' has the same structure as 'top_countries'
            for column in top_countries.columns:
                if column not in others_aggregated.columns:
                    others_aggregated = others_aggregated.with_columns(
                        pl.lit(None).cast(top_countries[column].dtype).alias(column)
                    )
                else:
                    others_aggregated = others_aggregated.with_columns(
                        pl.col(column).cast(top_countries[column].dtype)
                    )

            all_countries = top_countries.vstack(others_aggregated)

            # Add flag mapping
            all_countries = all_countries.with_columns(
                pl.col("flag").replace_strict(helper.flag_to_nationality_dict, default='Others').alias("country_full_name")
            )

            # Calculate percentage only for 'Unique Tracks' or 'Total Tracks'
            if mapped_metric_type in ["unique", "total"]:
                all_countries = all_countries.with_columns(
                    (pl.col("metric") / all_countries["metric"].sum() * 100).alias("percentage")
                )
            
           # Convert to Pandas for Plotly
            data_df = all_countries.to_pandas()
            data_df['order'] = data_df['flag'].apply(lambda x: 1 if x == "Others" else 0)
            data_df = data_df.sort_values(by=['order', 'metric'], ascending=[False, True])

            # Add hover text
            data_df["tooltip_text"] = data_df.apply(
                lambda row: (
                    f"<b>Flag:</b> {row['flag']}<br>"
                    f"<b>Country:</b> {row['country_full_name']}<br>"
                    f"<b>{mapped_metric_type.capitalize()}:</b> {helper.number_formatter(row['metric'])}<br>"
                    f"<b>Most Played Artist:</b> {row['most_played_artist']} "
                    f"({helper.number_formatter(row['most_played_count'])} plays)"
                ),
                axis=1
            )

            if mapped_metric_type == "average":
                data_df["label"] = data_df["metric"].apply(lambda x: f"{x:.1f}")
            else:
                data_df["label"] = data_df["percentage"].apply(lambda x: f"{x:.1f}%")

            # Plot
            fig = px.bar(
                data_df,
                x="metric",
                y='flag',
                text="label",
                title="",
                orientation='h',
                hover_data={"tooltip_text": True},
            )
            # Apply conditional coloring for "Portugal" or "PT"
            colors = [
                "#1f77b4" if country != "🇵🇹" else "#ff7f0e"  # Default color vs highlight color
                for country in data_df["flag"]
            ]
            fig.update_traces(
                marker_color=colors,
                textposition="outside",
                hovertemplate="%{customdata[0]}",
                customdata=data_df[["tooltip_text"]].to_numpy(),
                cliponaxis=False
            )
            fig.update_layout(
                xaxis_title=None,
                yaxis_title=None,
                margin=dict(l=10, r=30, t=30, b=0),
                height=400,
                hoverlabel_align="left",
            )
            st.plotly_chart(fig, use_container_width=True, key=f'{radio_name}_artists_by_country')


def display_artist_decades(app_config: dict, ncols: int, metric_type_option: str, mapped_metric_type: str):
    st.subheader(
        f':date: {st.session_state['metric_type']} Artists by :blue[*decade*]', 
        divider="gray",
        help="""Based on either the :blue-background[**birth year**] of the artists if they're a person
        or the :blue-background[**career start year**] of a group
        \n\nData Obtained from MusicBrainz"""
    )
    artist_decade_cols = st.columns(ncols)

    for i, (_, val) in enumerate(app_config.items()):
        with artist_decade_cols[i]:
            radio_name = val.get('name')
            radio_df = val.get('radio_df')

            # Calculate metrics by decade, including most played artist
            df_decades_artists = calculations.calculate_decade_metrics(
                _df=radio_df,
                date_column="mb_artist_career_begin",
                count_columns=[cm.ARTIST_NAME_COLUMN],
                metric_type=mapped_metric_type,
                include_most_played="artist",
                id=radio_name,
            )

            # Plot unique tracks by decade
            df_artists_decade_pandas = df_decades_artists.to_pandas()

            # Calculate percentage
            total_artists = df_artists_decade_pandas["metric"].sum()
            df_artists_decade_pandas["percentage"] = (
                df_artists_decade_pandas["metric"] / total_artists * 100
            ).apply(lambda x: f"{x:.1f}%")

            df_artists_decade_pandas["formatted_metric"] = df_artists_decade_pandas["metric"].apply(lambda x: helper.number_formatter(x))

            # Add hover text
            df_artists_decade_pandas["tooltip_text"] = df_artists_decade_pandas.apply(
                lambda row: (
                    f"<b>Decade:</b> {row['decade_year']}<br>"
                    f"<b>{metric_type_option} Artists:</b> {row['formatted_metric']}<br>"
                    f"<b>Percentage:</b> {row['percentage']}<br>"
                    f"<b>Most Played Artist:</b> {row['most_played_artist']} "
                    f"({helper.number_formatter(row['most_played_count'])} plays)"
                ),
                axis=1
            )
            
            fig_artists = px.bar(
                df_artists_decade_pandas,
                x="decade_year",
                y="metric",
                title="",
                labels={"decade_year": "Decade", "metric": "Artists"},
                orientation='v',
                text="percentage",
                hover_data={"tooltip_text": True},
            )
            fig_artists.update_traces(
                hovertemplate="%{customdata[0]}",
                customdata=df_artists_decade_pandas[["tooltip_text"]].to_numpy(),
                texttemplate="%{text}",
                textposition="outside"
            )
            fig_artists.update_layout(
                xaxis_title=None,
                yaxis_title=None,
                margin=dict(l=10, r=30, t=30, b=0),
                height=400,
                xaxis=dict(type='category', categoryorder='array', categoryarray=df_artists_decade_pandas["decade_year"].tolist()),  # Ensure proper ordering
                hoverlabel_align="left",
            )
            st.plotly_chart(fig_artists, use_container_width=True, key=f'{radio_name}_artists_by_decade')


def display_track_duration(app_config: dict, ncols: int, metric_type_option: str, mapped_metric_type: str):
    st.subheader(
        f':clock4: :blue[Track Duration]', 
        divider='gray',
        help='Truncated based on the number of minutes of the song. The duration of the song is extracted from :green[Spotify]'
    )
    st.caption('*Duration in minutes*')
    track_duration_cols = st.columns(ncols)

    for i, (_, val) in enumerate(app_config.items()):
        with track_duration_cols[i]:
            radio_name = val.get('name')
            radio_df = val.get('radio_df')

            # Calculate duration metrics with most played track details
            df_duration_tracks = calculations.calculate_duration_metrics(
                _df=radio_df,
                duration_column='spotify_duration_ms',
                count_columns=[cm.TRACK_TITLE_COLUMN, cm.ARTIST_NAME_COLUMN],
                metric_type=mapped_metric_type,
                include_most_played=True,
                id=radio_name,
            )

            # Convert to Pandas for Plotly
            df_duration_pandas = df_duration_tracks.to_pandas()

            # Calculate percentage
            total_tracks = df_duration_pandas["metric"].sum()
            df_duration_pandas["percentage"] = (
                (df_duration_pandas["metric"] / total_tracks) * 100
            ).apply(lambda x: f"{x:.2f}%")

            # Add formatted metric and hover text
            df_duration_pandas["formatted_metric"] = df_duration_pandas["metric"].apply(lambda x: helper.number_formatter(x))
            df_duration_pandas["most_played_info"] = df_duration_pandas.apply(
                lambda row: (
                    f"{row['most_played_track']} | {row['most_played_artist']} ({helper.number_formatter(row['most_played_count'])} plays)"
                    if pd.notnull(row['most_played_track']) else "N/A"
                ),
                axis=1
            )
            df_duration_pandas["tooltip_text"] = df_duration_pandas.apply(
                lambda row: (
                    f"<b>Track Minute Duration:</b> {row['duration_minutes']}<br>"
                    f"<b>{metric_type_option} Tracks:</b> {row['formatted_metric']}<br>"
                    f"<b>Percentage:</b> {row['percentage']}<br>"
                    f"<b>Most Played Track:</b> {row['most_played_info']}"
                ),
                axis=1
            )

            # Plot duration-based bar chart
            fig_duration = px.bar(
                df_duration_pandas,
                x="duration_minutes",
                y="metric",
                title="",
                labels={"duration_minutes": "Track Duration (Minutes)", "metric": metric_type_option},
                orientation="v",
                text="formatted_metric",  # Use formatted metric on bars
                hover_data={"tooltip_text": True},  # Use custom tooltips
            )
            fig_duration.update_traces(
                # marker_color="#d3d3d3",  # Light gray for bars
                texttemplate="%{text}",
                textposition="outside",
                hovertemplate="%{customdata[0]}",
                customdata=df_duration_pandas[["tooltip_text"]].to_numpy(),  # Attach custom tooltip text
                cliponaxis=False  # Prevent labels from being clipped
            )
            fig_duration.update_layout(
                xaxis_title=None,
                yaxis_title=None,
                margin=dict(l=10, r=30, t=0, b=0),
                height=400,
                xaxis=dict(type="category"),  # Treat durations as categories
                yaxis=dict(title=metric_type_option, tickformat=","),
                hoverlabel_align="left"  # Ensure left alignment for tooltips
            )
            st.plotly_chart(fig_duration, use_container_width=True, key=f"{radio_name}_tracks_by_duration")


def display_top_genres(app_config: dict, ncols: int, metric_type_option: str, mapped_metric_type: str):
    st.subheader(
        f':musical_score: Top 10 :blue[Genres]', 
        divider="gray",
        help='Genre information gathered from :green[**Spotify**] and :blue-background[**based of the main genre of the artist**].'
    )
    genre_cols = st.columns(ncols)

    for i, (_, val) in enumerate(app_config.items()):
        with genre_cols[i]:
            radio_name = val.get('name')
            radio_df = val.get('radio_df')
            
            # Calculate genre metrics
            df_genres_cleaned = calculations.calculate_genre_metrics(
                _df=radio_df,
                genre_column='spotify_genres',
                count_columns=[cm.TRACK_TITLE_COLUMN, cm.ARTIST_NAME_COLUMN],
                metric_type=mapped_metric_type,
                include_most_played=True,
                id=radio_name,
            )

            # Convert to Pandas for Plotly
            df_genres_pandas = df_genres_cleaned.rename({"split_genres": "spotify_genres"}).to_pandas()

            # Add percentage column
            total_metric = df_genres_pandas["metric"].sum()
            df_genres_pandas["percentage"] = (
                df_genres_pandas["metric"] / total_metric * 100
            ).apply(lambda x: f"{x:.2f}%")

            # Add formatted columns for tooltips
            df_genres_pandas["formatted_metric"] = df_genres_pandas["metric"].apply(
                lambda x: helper.number_formatter(x)
            )
            df_genres_pandas["most_played_info"] = df_genres_pandas.apply(
                lambda row: (
                    f"{row['most_played_track']} | {row['most_played_artist']} ({helper.number_formatter(row['most_played_count'])} plays)"
                    if pd.notnull(row['most_played_track']) else "N/A"
                ),
                axis=1
            )
            df_genres_pandas["tooltip_text"] = df_genres_pandas.apply(
                lambda row: (
                    f"<b>Genre:</b> {row['spotify_genres']}<br>"
                    f"<b>{metric_type_option} Tracks:</b> {row['formatted_metric']}<br>"
                    f"<b>Percentage:</b> {row['percentage']}<br>"
                    f"<b>Most Played Track:</b> {row['most_played_info']}"
                ),
                axis=1
            )

            # Plot horizontal bar chart
            fig_genres = px.bar(
                df_genres_pandas,
                x="metric",
                y="spotify_genres",
                title="",
                labels={"metric": metric_type_option, "spotify_genres": "Genre"},
                orientation="h",
                text="formatted_metric",  # Show count on bars
                hover_data={"tooltip_text": True},  # Use custom tooltips
            )
            fig_genres.update_traces(
                texttemplate="%{text}", 
                textposition="outside",
                hovertemplate="%{customdata[0]}",
                customdata=df_genres_pandas[["tooltip_text"]].to_numpy(),  # Attach custom tooltip text
                cliponaxis=False  # Prevent labels from being clipped
            )
            fig_genres.update_layout(
                xaxis_title=None,
                yaxis_title=None,
                margin=dict(l=150, r=30, t=0, b=10),  # Adjust for long genre names
                height=400,
                hoverlabel_align = 'left',
            )
            st.plotly_chart(fig_genres, use_container_width=True, key=f"{radio_name}_top_genres")


def display_sentiment_analysis(app_config: dict, ncols: int, global_max_mean_values: dict):
    st.subheader(
        f'😊 :blue[Sentiment Analysis]', 
        divider="gray",
        help="""These plots show the relative strength of various sentiment metrics (Joy, Sadness, Optimism, Anger, and Love) 
        :blue-background[**calculated from the song lyrics played**]. Sentiments are derived using NLP models, while the 
        "Love Mentions" metric represents the frequency of words related to love across the lyrics. Values are normalized 
        based on the maximum mean sentiment value across all radios for comparability.
        """
    )
    sentiment_cols = st.columns(ncols)

    for i, (_, val) in enumerate(app_config.items()):
        with sentiment_cols[i]:
            radio_name = val.get('name')
            mean_values = val.get('mean_values')  # Retrieve mean values for this radio
            radio_csv = val.get('radio_csv')
            
            # Normalize mean values using global max mean values
            normalized_values = [
                mean_values[metric][0] / global_max_mean_values[metric][0]
                for metric in mean_values.keys()
            ]

            # Close the loop for the radar chart
            normalized_values.append(normalized_values[0])  # Repeat the first value to close the radar chart
            categories = list(mean_values.keys()) + [list(mean_values.keys())[0]]  # Repeat the first category
            categories = list(mappings.category_map.values()) + [list(mappings.category_map.values())[0]]  # Map to display labels

            # Create radar chart
            fig = go.Figure()
            fig.add_trace(go.Scatterpolar(
                r=normalized_values,
                theta=categories,
                fill='toself',
                # name=f'{radio_name}'
                name=None,
            ))

            # Update layout for radar chart
            fig.update_layout(
                polar=dict(
                    radialaxis=dict(
                        visible=True,
                        range=[0, 1]  # Ensure all metrics are on the same scale (0 to 1)
                    )
                ),
                # showlegend=True,
                # title=f"Lyrics Sentiment Star Plot for {radio_name}"
                margin=dict(l=100, r=100, t=50, b=50),  # Adjust for long genre names
                height=400,
                hoverlabel_align = 'left',
            )

            # Display radar chart in Streamlit
            st.plotly_chart(fig, use_container_width=True, key=f"{radio_name}_radar_chart")

            st.divider()

            # Add an export button for the radio CSV data
            # csv = radio_df.to_pandas().to_csv(index=False)  # Convert to pandas and CSV
            with stylable_container(
                key=f'csv_export_button',
                css_styles="""
                    button {
                        width: 275px;
                        height: 60px;
                        background-color: #add8e6; /* Light Blue */
                        color: white;
                        border-radius: 5px;
                        white-space: nowrap;
                    }
                    """,
            ):
                _, csv_col, _ = st.columns([1,2,1])
                with csv_col:
                    st.download_button(
                        label=f"Export :grey-background[**{radio_name}**] Data as CSV",
                        data=radio_csv,
                        file_name=f"{radio_name}_data.csv",
                        mime="text/csv",
                        key=f"{radio_name}_export_button"
                    )