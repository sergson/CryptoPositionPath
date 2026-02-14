# svg_track_renderer.py
"""
SVG renderer for displaying tracks
"""
from typing import Dict, List, Tuple, Optional
import streamlit as st
from datetime import datetime, timedelta
from track_builder import TrackSegment
import streamlit.components.v1 as components
from logger import perf_logger
import time
import pandas as pd


class SVGTrackRenderer:
    """SVG renderer for tracks"""

    def __init__(self, storage):
        self.storage = storage
        self.logger = perf_logger.get_logger('svg_track_renderer', 'render')
        self._init_session_state()
        self._load_settings()

    def render_tracks_svg(self, exchange: str, market_type: str,
                          width: int = 800, height: int = 800,
                          show_grid: bool = True,
                          filter_minutes: int = 1440,
                          show_manual: bool = True,
                          show_auto: bool = True,
                          show_up: bool = True,
                          show_flat: bool = True,
                          show_down: bool = True,
                          min_volume: float = 0,
                          min_rank_change: float = 0) -> str:  # Added parameter
        """
        Generate SVG with tracks using additional filters
        """
        start_time = time.time()

        try:
            from track_builder import TrackBuilder
            track_builder = TrackBuilder(self.storage)

            # Convert minutes to hours for loading from DB (with buffer)
            lookback_hours = max(1, (filter_minutes + 59) // 60)

            # Load tracks from DB with filtering by exchange time
            all_tracks = track_builder.load_tracks_from_db(
                exchange, market_type, lookback_hours=lookback_hours
            )

            self.logger.debug(f"Loaded tracks from DB: {sum(len(tracks) for tracks in all_tracks.values())}")
            for pair, track_list in all_tracks.items():
                self.logger.debug(f"  Pair {pair}: {len(track_list)} tracks")
                manual_count = sum(1 for t in track_list if t.track_type == 'manual')
                auto_count = sum(1 for t in track_list if t.track_type == 'auto')
                self.logger.debug(f"    Manual: {manual_count}, Auto: {auto_count}")

            if not all_tracks:
                return self._create_empty_svg(width, height)

            # Filter tracks by time (in minutes)
            filtered_tracks = self._filter_recent_tracks(all_tracks, filter_minutes)

            # Additional filtering by type and direction
            filtered_tracks = self._filter_tracks_by_type_and_direction(
                filtered_tracks, show_manual, show_auto, show_up, show_flat, show_down
            )

            # Filter by minimum volume
            if min_volume > 0:
                filtered_tracks = self._filter_tracks_by_volume(filtered_tracks, min_volume)

            # Filter by minimum rank change
            if min_rank_change > 0:
                filtered_tracks = self._filter_tracks_by_rank_change(filtered_tracks, min_rank_change)

            # Generate SVG
            svg_content = self._generate_svg_content(
                filtered_tracks, width, height, show_grid
            )

            elapsed = time.time() - start_time
            track_count = sum(len(tracks) for tracks in filtered_tracks.values())
            self.logger.debug(
                f"Generated SVG with {track_count} tracks in {elapsed:.3f} sec "
                f"(filter: {filter_minutes} min, min volume: {min_volume}, min rank change: {min_rank_change})")

            return svg_content

        except ImportError as e:
            self.logger.error(f"Error importing track_builder: {e}")
            return self._create_empty_svg(width, height)
        except Exception as e:
            self.logger.error(f"Error rendering tracks: {e}")
            return self._create_error_svg(width, height, str(e))

    def _init_session_state(self):
        """Initialize session state with default values"""
        defaults = {
            'tracks_filter_minutes': 60,
            'tracks_min_volume': 0,
            'tracks_min_rank_change': 0,
            'tracks_show_manual': True,
            'tracks_show_auto': True,
            'tracks_show_up_filter': True,
            'tracks_show_flat_filter': True,
            'tracks_show_down_filter': True
        }

        for key, default_value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = default_value

    def _save_setting_on_change(self, key: str, value: any):
        """Save setting on change"""
        st.session_state[key] = value
        self.storage.save_setting(key, value)

    def _load_settings(self):
        """Load saved settings on startup"""
        if not st.session_state.get("loaded_tracks_settings", False):
            st.session_state.loaded_tracks_settings = True

            # Load settings from database
            settings = self.storage.get_all_settings()

            # Ensure settings exist in database
            for key in ['tracks_filter_minutes', 'tracks_min_volume',
                        'tracks_min_rank_change', 'tracks_show_manual', 'tracks_show_auto',
                        'tracks_show_up_filter', 'tracks_show_flat_filter', 'tracks_show_down_filter',
                        'tracks_show_grid','tracks_width', 'tracks_height']:
                if key in settings:
                    value = settings[key]
                    # Make sure value is correct
                    if value is not None:
                        st.session_state[key] = value
                    else:
                        # If value is None, use default
                        default_values = {
                            'tracks_filter_minutes': 60,
                            'tracks_min_volume':0,
                            'tracks_min_rank_change':0,
                            'tracks_show_manual': True,
                            'tracks_show_auto': True,
                            'tracks_show_up_filter': True,
                            'tracks_show_flat_filter': True,
                            'tracks_show_down_filter': True,
                            'tracks_show_grid': True,
                            'tracks_width': 800,
                            'tracks_height': 800
                        }
                        if key in default_values:
                            st.session_state[key] = default_values[key]

    def _filter_tracks_by_type_and_direction(self, tracks: Dict[str, List[TrackSegment]],
                                             show_manual: bool,
                                             show_auto: bool,
                                             show_up: bool,
                                             show_flat: bool,
                                             show_down: bool) -> Dict[str, List[TrackSegment]]:
        """Filter tracks by type and direction"""
        filtered = {}

        for pair, track_list in tracks.items():
            filtered_tracks = []

            for track in track_list:
                # Filter by track type
                if track.track_type == 'manual' and not show_manual:
                    continue
                if track.track_type == 'auto' and not show_auto:
                    continue

                # Filter by direction for auto tracks
                if track.track_type == 'auto':
                    if track.direction == 'up' and not show_up:
                        continue
                    if track.direction == 'flat' and not show_flat:
                        continue
                    if track.direction == 'down' and not show_down:
                        continue

                filtered_tracks.append(track)

            if filtered_tracks:
                filtered[pair] = filtered_tracks

        return filtered

    def _filter_tracks_by_volume(self, tracks: Dict[str, List[TrackSegment]],
                                 min_volume: float) -> Dict[str, List[TrackSegment]]:
        """Filter tracks by minimum volume"""
        filtered = {}

        for pair, track_list in tracks.items():
            filtered_tracks = []

            for track in track_list:
                # Filter by track type
                if track.track_type == 'manual':
                    filtered_tracks.append(track)
                    continue
                # Check if track has points and last point volume
                if track.points and len(track.points) > 0:
                    last_point = track.points[-1]
                    if last_point.volume >= min_volume:
                        filtered_tracks.append(track)

            if filtered_tracks:
                filtered[pair] = filtered_tracks

        return filtered

    def _filter_tracks_by_rank_change(self, tracks: Dict[str, List[TrackSegment]],
                                      min_rank_change: float) -> Dict[str, List[TrackSegment]]:
        """Filter tracks by minimum rank change (threshold)"""
        filtered = {}

        for pair, track_list in tracks.items():
            filtered_tracks = []

            for track in track_list:
                # Filter by track type
                if track.track_type == 'manual':
                    filtered_tracks.append(track)
                    continue
                # Calculate rank change (absolute value)
                rank_change = abs(track.start_rank - track.end_rank)
                if rank_change >= min_rank_change:
                    filtered_tracks.append(track)

            if filtered_tracks:
                filtered[pair] = filtered_tracks

        return filtered

    def _filter_recent_tracks(self, all_tracks: Dict[str, List[TrackSegment]],
                              minutes: int = 1440) -> Dict[str, List[TrackSegment]]:
        """Filter tracks by time (in minutes)"""
        if minutes <= 0:
            # If 0 or less - show all tracks
            return all_tracks

        from datetime import timezone
        cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=minutes)

        filtered = {}
        for pair, tracks in all_tracks.items():
            recent_tracks = []
            for track in tracks:
                # Ensure track time has timezone
                if track.end_time.tzinfo is None:
                    # If no timezone, add UTC
                    track_end_time = track.end_time.replace(tzinfo=timezone.utc)
                else:
                    track_end_time = track.end_time

                if track_end_time >= cutoff_time:
                    recent_tracks.append(track)

            if recent_tracks:
                filtered[pair] = recent_tracks

        return filtered

    def _generate_svg_content(self, tracks: Dict[str, List[TrackSegment]],
                              width: int, height: int, show_grid: bool) -> str:
        """Generate SVG content with labeled axes on black background"""

        # Find time and rank ranges
        time_range, rank_range = self._calculate_ranges(tracks)

        min_time, max_time = time_range
        min_rank, max_rank = rank_range

        svg_parts = [
            f'<svg width="{width}" height="{height}" '
            f'viewBox="0 0 {width} {height}" '
            'xmlns="http://www.w3.org/2000/svg" style="background: #000;">',  # Black background

            '<style>',
            '  .track-path {',
            '    fill: none;',
            '    stroke-width: 2;',
            '    stroke-linecap: round;',
            '    stroke-linejoin: round;',
            '    cursor: pointer;',
            '  }',
            '  .track-path:hover {',
            '    stroke-width: 4;',
            '    opacity: 1;',
            '    filter: drop-shadow(0 0 3px currentColor);',
            '  }',
            '  .auto-track {',
            '    stroke-width: 2;',
            '    opacity: 0.9;',
            '  }',
            '  .manual-track {',
            '    stroke-width: 2;',
            '    stroke-dasharray: 5,5;',
            '    opacity: 0.8;',
            '  }',
            '  .grid-line {',
            '    stroke: #555;',  # Lighter for black background
            '    stroke-width: 1;',
            '    opacity: 0.4;',
            '  }',
            '  .axis-line {',
            '    stroke: #777;',  # Lighter for black background
            '    stroke-width: 2;',
            '  }',
            '  .axis-label {',
            '    font-size: 12px;',
            '    fill: #aaa;',  # Lighter for black background
            '    font-family: Arial, sans-serif;',
            '  }',
            '  .time-label {',
            '    font-size: 10px;',
            '    fill: #888;',  # Lighter for black background
            '    text-anchor: middle;',
            '  }',
            '  .rank-label {',
            '    font-size: 10px;',
            '    fill: #888;',  # Lighter for black background
            '    text-anchor: end;',
            '  }',
            '  .direction-indicator {',
            '    font-size: 9px;',
            '    fill: #4fc3f7;',
            '    font-weight: bold;',
            '  }',
            '</style>'
        ]

        # Grid
        if show_grid:
            svg_parts.extend(self._generate_grid(width, height, time_range, rank_range))

        # Axes
        svg_parts.extend(self._generate_axes(width, height, min_time, max_time, min_rank, max_rank))

        # Tracks
        for pair, track_list in tracks.items():
            for track in track_list:
                if track.track_type == 'manual':
                    track_svg = self._render_manual_track(track, width, height, time_range, rank_range)
                else:
                    track_svg = self._render_auto_track(track, width, height, time_range, rank_range)
                svg_parts.append(track_svg)

        svg_parts.append('</svg>')

        return '\n'.join(svg_parts)

    def _generate_axes(self, width: int, height: int,
                       min_time: datetime, max_time: datetime,
                       min_rank: int, max_rank: int) -> List[str]:
        """Generate axes with inverted rank axis"""
        svg = []

        # X axis (time) - bottom
        svg.append(f'<line class="axis-line" x1="0" y1="{height}" x2="{width}" y2="{height}"/>')

        # Y axis (rank) - left, with inverted scale
        svg.append(f'<line class="axis-line" x1="0" y1="0" x2="0" y2="{height}"/>')

        # Direction arrows
        svg.append(f'<text class="direction-indicator" x="{width - 40}" y="{height - 15}">time →</text>')
        svg.append(f'<text class="direction-indicator" x="30" y="15">↑ growth</text>')

        # Range information
        time_range_str = f"{min_time.strftime('%H:%M')} - {max_time.strftime('%H:%M')}"
        rank_range_str = f"{min_rank} (best) - {max_rank} (worst)"

        svg.append(f'<text class="time-label" x="{width / 2}" y="15">Period: {time_range_str}</text>')

        return svg

    def _normalize_datetime(self, dt: datetime) -> datetime:
        """Normalize datetime to aware UTC"""
        from datetime import timezone

        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt

    def _calculate_ranges(self, tracks: Dict[str, List[TrackSegment]]) -> Tuple[Tuple, Tuple]:
        """Calculate time and rank ranges with padding"""
        from datetime import timezone

        all_times = []
        all_ranks = []

        for track_list in tracks.values():
            for track in track_list:
                for point in track.points:
                    # Ensure time has timezone
                    point_time = point.time
                    if point_time.tzinfo is None:
                        point_time = point_time.replace(tzinfo=timezone.utc)
                    all_times.append(point_time)
                all_ranks.extend([p.rank for p in track.points])

        if not all_times:
            # Return reasonable default values with timezone
            default_min = datetime.now(timezone.utc) - timedelta(hours=168)
            default_max = datetime.now(timezone.utc)
            return ((default_min, default_max), (1, 100))

        min_time = min(all_times)
        max_time = max(all_times)
        min_rank = min(all_ranks)
        max_rank = max(all_ranks)

        # Add padding
        time_padding = (max_time - min_time) * 0.1
        rank_padding = max(10, (max_rank - min_rank) * 0.1)

        return (
            (min_time - time_padding, max_time + time_padding),
            (max(1, min_rank - rank_padding), max_rank + rank_padding)
        )

    def _generate_grid(self, width: int, height: int,
                       time_range: Tuple[datetime, datetime], rank_range: Tuple[int, int]) -> List[str]:
        """Generate grid with inverted rank axis"""
        min_time, max_time = time_range
        min_rank, max_rank = rank_range

        time_span = (max_time - min_time).total_seconds() / 60
        rank_span = max_rank - min_rank

        svg = []

        # Vertical lines (every hour)
        hours = max(1, int(time_span / 60))
        for hour in range(0, hours + 1):
            x = int((hour * 60) / time_span * width)
            svg.append(f'<line class="grid-line" x1="{x}" y1="0" x2="{x}" y2="{height}"/>')

            label_time = (min_time + timedelta(hours=hour)).strftime('%H:%M')
            svg.append(f'<text class="time-label" x="{x}" y="{height - 5}">{label_time}</text>')

        # Horizontal lines (ranks) - INVERTED AXIS
        rank_step = 20  # Every 20 ranks
        min_rank_int = int(min_rank)
        max_rank_int = int(max_rank)

        for rank in range(min_rank_int, max_rank_int + 1, rank_step):
            if rank > max_rank_int:
                continue

            # INVERSION: lower rank means higher line
            y = int(((rank - min_rank) / rank_span) * height)
            svg.append(f'<line class="grid-line" x1="0" y1="{y}" x2="{width}" y2="{y}"/>')
            svg.append(f'<text class="rank-label" x="25" y="{y - 3}">{rank}</text>')

        return svg

    def _render_auto_track(self, track: TrackSegment,
                           width: int, height: int,
                           time_range: Tuple, rank_range: Tuple) -> str:
        """Render an auto track with inverted rank axis"""
        min_time, max_time = time_range
        min_rank, max_rank = rank_range

        time_span = (max_time - min_time).total_seconds() / 60
        rank_span = max_rank - min_rank

        points = []
        for point in track.points:
            x = int(((point.time - min_time).total_seconds() / 60) / time_span * width)
            y = int(((point.rank - min_rank) / rank_span) * height)
            points.append(f"{x},{y}")

        points_str = " ".join(points)

        # Take last point for tooltip data
        last_point = track.points[-1] if track.points else None
        if last_point:
            last_price = last_point.price
            last_change = last_point.change
            last_volume = last_point.volume
            last_time_utc = last_point.time.replace(tzinfo=None).isoformat() + 'Z'
        else:
            last_price = last_change = last_volume = 0
            last_time_utc = ""

        return (
            f'<polyline class="track-path auto-track" '
            f'points="{points_str}" '
            f'stroke="{track.color}" '
            f'data-pair="{track.pair}" '
            f'data-direction="{track.direction}" '
            f'data-start-rank="{track.start_rank}" '
            f'data-end-rank="{track.end_rank}" '
            f'data-start-time="{track.start_time.replace(tzinfo=None).isoformat()}Z" '
            f'data-end-time="{track.end_time.replace(tzinfo=None).isoformat()}Z" '
            f'data-points-count="{len(track.points)}" '
            f'data-last-price="{last_price}" '
            f'data-last-change="{last_change}" '
            f'data-last-volume="{last_volume}" '
            f'data-last-time="{last_time_utc}" '
            f'onmouseover="showTooltip(this)" '
            f'onmouseout="hideTooltip()"/>'
        )

    def _render_manual_track(self, track: TrackSegment,
                             width: int, height: int,
                             time_range: Tuple, rank_range: Tuple) -> str:
        """Render a manual track with inverted rank axis"""
        min_time, max_time = time_range
        min_rank, max_rank = rank_range

        time_span = (max_time - min_time).total_seconds() / 60
        rank_span = max_rank - min_rank

        points = []
        for point in track.points:
            x = int(((point.time - min_time).total_seconds() / 60) / time_span * width)
            y = int(((point.rank - min_rank) / rank_span) * height)
            points.append(f"{x},{y}")

        points_str = " ".join(points)

        # Take data for tooltip
        last_point = track.points[-1] if track.points else None
        if last_point:
            last_price = last_point.price
            last_change = last_point.change
            last_volume = last_point.volume
            last_time_utc = last_point.time.replace(tzinfo=None).isoformat() + 'Z'
        else:
            last_price = last_change = last_volume = 0
            last_time_utc = ""

        return (
            f'<polyline class="track-path manual-track" '
            f'points="{points_str}" '
            f'stroke="{track.color}" '
            f'data-pair="{track.pair}" '
            f'data-type="manual" '
            f'data-is-manual="true" '
            f'data-start-rank="{track.start_rank}" '
            f'data-end-rank="{track.end_rank}" '
            f'data-start-time="{track.start_time.replace(tzinfo=None).isoformat()}Z" '
            f'data-end-time="{track.end_time.replace(tzinfo=None).isoformat()}Z" '
            f'data-points-count="{len(track.points)}" '
            f'data-last-price="{last_price}" '
            f'data-last-change="{last_change}" '
            f'data-last-volume="{last_volume}" '
            f'data-last-time="{last_time_utc}" '
            f'onmouseover="showTrackTooltip(this, event)" '
            f'onmouseout="hideTrackTooltip()"/>'
        )

    def _create_empty_svg(self, width: int, height: int) -> str:
        """Create empty SVG"""
        return f'''
        <svg width="{width}" height="{height}" 
             viewBox="0 0 {width} {height}"
             xmlns="http://www.w3.org/2000/svg">
            <rect width="100%" height="100%" fill="#f8f9fa"/>
            <text x="50%" y="50%" text-anchor="middle" 
                  font-family="Arial" font-size="16" fill="#6c757d">
                No data to display tracks
            </text>
        </svg>
        '''

    def _create_error_svg(self, width: int, height: int, error_msg: str) -> str:
        """Create SVG with error"""
        return f'''
        <svg width="{width}" height="{height}" 
             viewBox="0 0 {width} {height}"
             xmlns="http://www.w3.org/2000/svg">
            <rect width="100%" height="100%" fill="#f8d7da"/>
            <text x="50%" y="50%" text-anchor="middle" 
                  font-family="Arial" font-size="14" fill="#721c24">
                Error: {error_msg[:50]}...
            </text>
        </svg>
        '''

    def display_tracks_in_streamlit(self, exchange: str, market_type: str):
        """Display tracks in Streamlit with pan and zoom"""
        from datetime import timezone

        st.subheader("📈 Pair position trajectory tracks in price growth chart for selected period")

        # Initialize update state
        if 'tracks_refresh_counter' not in st.session_state:
            st.session_state.tracks_refresh_counter = 0
        if 'last_tracks_update' not in st.session_state:
            st.session_state.last_tracks_update = time.time()
        if 'tracks_auto_refresh' not in st.session_state:
            st.session_state.tracks_auto_refresh = True

        # IMPORTANT: Fix initialization - use tracks_filter_minutes instead of tracks_filter_hours
        if 'tracks_filter_minutes' not in st.session_state:
            st.session_state.tracks_filter_minutes = 60  # Default 24 hours (1440 minutes)

        # Initialize minimum volume
        if 'tracks_min_volume' not in st.session_state:
            st.session_state.tracks_min_volume = 0  # Default show all

        # Initialize minimum rank change
        if 'tracks_min_rank_change' not in st.session_state:
            st.session_state.tracks_min_rank_change = 0  # Default show all

        # Filter controls
        with st.expander("⚙️ Filter Settings", expanded=False):
            col1, col2 = st.columns(2)

            with col1:
                # Time filter (in minutes)
                filter_minutes = st.slider(
                    "Display period (minutes)",
                    min_value=10,
                    max_value=1440,
                    value=st.session_state.tracks_filter_minutes,
                    step=5,
                    key="filter_minutes_slider",
                    help="Show tracks only for the specified period"
                )
                st.session_state.tracks_filter_minutes = filter_minutes
                self._save_setting_on_change('tracks_filter_minutes', filter_minutes)

                # Period description in minutes/hours
                hours = filter_minutes / 60
                if hours < 1:
                    period_desc = f"{filter_minutes} minutes"
                elif hours == 1:
                    period_desc = "1 hour"
                elif hours < 24:
                    period_desc = f"{hours:.1f} hours"
                else:
                    days = hours / 24
                    period_desc = f"{days:.1f} days"

                st.caption(f"⏱️ Showing tracks from the last {period_desc}")

                # Minimum volume filter
                st.markdown("---")
                st.write("**Volume filter:**")
                min_volume = st.slider(
                    "Minimum volume (24h)",
                    min_value=0,
                    max_value=10000000,
                    value=st.session_state.tracks_min_volume,
                    step=100000,
                    key="min_volume_slider",
                    help="Show tracks with volume not less than specified"
                )
                st.session_state.tracks_min_volume = min_volume
                self._save_setting_on_change('tracks_min_volume', min_volume)

                # Formatted volume output
                if min_volume == 0:
                    volume_desc = "All tracks"
                elif min_volume < 1000:
                    volume_desc = f"{min_volume:.0f}"
                elif min_volume < 1000000:
                    volume_desc = f"{min_volume / 1000:.1f}K"
                else:
                    volume_desc = f"{min_volume / 1000000:.2f}M"

                st.caption(f"📊 Min volume: {volume_desc}")

                # Minimum rank change filter
                st.write("**Rank change filter:**")
                min_rank_change = st.slider(
                    "Minimum rank change",
                    min_value=0,
                    max_value=500,
                    value=st.session_state.tracks_min_rank_change,
                    step=1,
                    key="min_rank_change_slider",
                    help="Show tracks with rank change not less than specified value (absolute)"
                )
                st.session_state.tracks_min_rank_change = min_rank_change
                self._save_setting_on_change('tracks_min_rank_change', min_rank_change)

                st.caption(f"📈 Min rank change: {min_rank_change} positions")
            with col2:
                # Filter by track type
                st.markdown("---")
                st.write("**Track type:**")
                show_manual = st.checkbox("Manual tracks", value=st.session_state.tracks_show_manual, key="show_manual_filter")
                st.session_state.tracks_show_manual = show_manual
                self._save_setting_on_change('tracks_show_manual', show_manual)
                show_auto = st.checkbox("Auto tracks", value=st.session_state.tracks_show_auto, key="show_auto_filter")
                st.session_state.tracks_show_auto = show_auto
                self._save_setting_on_change('tracks_show_auto', show_auto)

                # Filter by direction
                st.write("**Direction:**")
                show_up = st.checkbox("Rising (📈)", value=st.session_state.tracks_show_up_filter, key="show_up_filter")
                st.session_state.tracks_show_up_filter = show_up
                self._save_setting_on_change('tracks_show_up_filter', show_up)
                show_flat = st.checkbox("Flat (➡️)", value=st.session_state.tracks_show_flat_filter, key="show_flat_filter")
                st.session_state.tracks_show_flat_filter = show_flat
                self._save_setting_on_change('tracks_show_flat_filter', show_flat)
                show_down = st.checkbox("Falling (📉)", value=True, key="st.session_state.tracks_show_down")
                st.session_state.tracks_show_down_filter = show_down
                self._save_setting_on_change('tracks_show_down_filter', show_down)

        # Calculate cutoff_time with timezone
        cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=st.session_state.tracks_filter_minutes)

        # Control buttons
        refresh_col1, refresh_col2 = st.columns([3, 1])
        with refresh_col1:
            if st.button("🔄 **Refresh**", key="manual_refresh_tracks",
                         help="Refresh track data"):
                st.session_state.tracks_refresh_counter += 1
                st.session_state.last_tracks_update = time.time()
                st.rerun()

        col1, col2 = st.columns(2)

        with col1:
            proportions = st.slider("SVG proportions", 1, 200, 100, 10, key="svg_proportions")
            st.session_state.tracks_width = 800 * (proportions / 100)
            self._save_setting_on_change('tracks_width', st.session_state.tracks_width)
            st.session_state.tracks_height = 800 / (proportions / 100)
            self._save_setting_on_change('tracks_height', st.session_state.tracks_height)
        with col2:
            st.session_state.tracks_show_grid = st.checkbox("Show grid", value=True, key="show_grid")
            self._save_setting_on_change('tracks_show_grid', st.session_state.tracks_show_grid)

            # Old option for backward compatibility
            if 'show_manual' in st.session_state:
                show_manual_deprecated = st.session_state.show_manual
            else:
                show_manual_deprecated = True

        # Generate SVG with filters
        try:
            svg_content = self.render_tracks_svg(
                exchange, market_type,
                width=st.session_state.tracks_width,
                height=st.session_state.tracks_height,
                show_grid=st.session_state.tracks_show_grid,
                filter_minutes=st.session_state.tracks_filter_minutes,
                show_manual=st.session_state.tracks_show_manual,
                show_auto=st.session_state.tracks_show_auto,
                show_up=st.session_state.tracks_show_up_filter,
                show_flat=st.session_state.tracks_show_flat_filter,
                show_down=st.session_state.tracks_show_down_filter,
                min_volume=st.session_state.tracks_min_volume,
                min_rank_change=st.session_state.tracks_min_rank_change  # Added parameter
            )
        except Exception as e:
            st.error(f"SVG rendering error: {e}")
            svg_content = self._create_error_svg(st.session_state.tracks_width, st.session_state.tracks_height, str(e))

        # Display data info with filters
        try:
            from track_builder import TrackBuilder
            track_builder = TrackBuilder(self.storage)

            # Convert minutes to hours for loading from DB
            lookback_hours = max(1, (st.session_state.tracks_filter_minutes + 59) // 60)

            all_tracks = track_builder.load_tracks_from_db(
                exchange, market_type, lookback_hours=lookback_hours
            )

            if all_tracks:
                # All tracks in loaded period
                total_all_tracks = sum(len(tracks) for tracks in all_tracks.values())

                # Filtered tracks by time
                filtered_by_time = self._filter_recent_tracks(all_tracks, st.session_state.tracks_filter_minutes)

                # Additional filtering by type and direction
                final_filtered_tracks = self._filter_tracks_by_type_and_direction(
                    filtered_by_time, show_manual, show_auto, show_up, show_flat, show_down
                )

                # Filter by volume
                final_filtered_tracks = self._filter_tracks_by_volume(
                    final_filtered_tracks, st.session_state.tracks_min_volume
                )

                # Filter by rank change
                final_filtered_tracks = self._filter_tracks_by_rank_change(
                    final_filtered_tracks, st.session_state.tracks_min_rank_change
                )

                total_filtered_tracks = sum(len(tracks) for tracks in final_filtered_tracks.values())
                total_filtered_pairs = len(final_filtered_tracks)

                # Statistics by type and direction
                manual_count = sum(len([t for t in tracks if t.track_type == 'manual']) for tracks in
                                   final_filtered_tracks.values())
                auto_count = sum(
                    len([t for t in t_list if t.track_type == 'auto']) for t_list in final_filtered_tracks.values())
                up_count = sum(
                    len([t for t in tracks if t.direction == 'up']) for tracks in final_filtered_tracks.values())
                down_count = sum(
                    len([t for t in tracks if t.direction == 'down']) for tracks in final_filtered_tracks.values())
                flat_count = total_filtered_tracks - up_count - down_count

                # Info message
                info_text = f"✅ Loaded {total_filtered_tracks} tracks from {total_filtered_pairs} pairs"
                info_text += f" (filter: {st.session_state.tracks_filter_minutes} min"

                if st.session_state.tracks_min_volume > 0:
                    volume_text = f"{st.session_state.tracks_min_volume / 1000000:.2f}M" if st.session_state.tracks_min_volume >= 1000000 else \
                        f"{st.session_state.tracks_min_volume / 1000:.0f}K" if st.session_state.tracks_min_volume >= 1000 else \
                            f"{st.session_state.tracks_min_volume:.0f}"
                    info_text += f", min volume: {volume_text}"

                if st.session_state.tracks_min_rank_change > 0:
                    info_text += f", min rank change: {st.session_state.tracks_min_rank_change}"

                info_text += f", total in DB: {total_all_tracks} tracks)"

                st.success(info_text)

                # Show last update date
                last_update_time = datetime.fromtimestamp(st.session_state.last_tracks_update)
                st.caption(f"Last update: {last_update_time.strftime('%H:%M:%S')}")

                # Time range info
                cutoff_time = datetime.now() - timedelta(minutes=st.session_state.tracks_filter_minutes)
                st.caption(f"Showing tracks from: {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                st.warning("⚠️ No track data.")
        except Exception as e:
            st.error(f"Error loading track data: {e}")

        # Add container for SVG with fixed size and black background
        container_height = min(st.session_state.tracks_height + 50, 900)
        container_style = f"""
        <style>
        .svg-container {{
            width: 100%;
            height: {container_height}px;
            overflow: auto;
            border: 1px solid #444;
            border-radius: 5px;
            background: #000 !important;
            margin-bottom: 20px;
            cursor: grab;
        }}
        .svg-container:active {{
            cursor: grabbing;
        }}
        .svg-content {{
            min-width: {st.session_state.tracks_width}px;
            min-height: {st.session_state.tracks_height}px;
            background: #000;
        }}
        .controls {{
            position: fixed;
            bottom: 20px;
            right: 20px;
            z-index: 1000;
        }}
        .control-btn {{
            background: rgba(255,255,255,0.2);
            color: white;
            border: 1px solid #666;
            border-radius: 50%;
            width: 40px;
            height: 40px;
            margin: 5px;
            cursor: pointer;
            font-size: 20px;
            transition: all 0.2s;
        }}
        .control-btn:hover {{
            background: rgba(255,255,255,0.3);
            transform: scale(1.1);
        }}
        .tooltip {{
            position: fixed;
            background: rgba(20,20,20,0.95);
            color: white;
            padding: 8px 12px;
            border-radius: 6px;
            font-size: 12px;
            pointer-events: none;
            z-index: 10000;
            border: 1px solid #444;
            box-shadow: 0 4px 12px rgba(0,0,0,0.7);
            max-width: 300px;
            display: none;
            backdrop-filter: blur(5px);
            font-family: Arial, sans-serif;
        }}
        .tooltip strong {{
            color: #4fc3f7;
        }}
        </style>
        """

        # Add JavaScript for panning, zooming and tooltips
        js_code = '''
        <script>
        // Global variables for view management
        let scale = 1;
        let translateX = 0;
        let translateY = 0;
        let isPanning = false;
        let startX = 0;
        let startY = 0;
        let currentTooltip = null;

        function saveViewState() {
            sessionStorage.setItem('svgViewState', JSON.stringify({
                scale: scale,
                translateX: translateX,
                translateY: translateY
            }));
        }

        function loadViewState() {
            const saved = sessionStorage.getItem('svgViewState');
            if (saved) {
                try {
                    const state = JSON.parse(saved);
                    scale = state.scale || 1;
                    translateX = state.translateX || 0;
                    translateY = state.translateY || 0;
                } catch (e) {
                    console.log('Error loading view state:', e);
                }
            }
        }

    function showTrackTooltip(element, clientX, clientY) {
    if (isPanning) return;

    let tooltip = document.getElementById('track-tooltip');
    if (!tooltip) {
        tooltip = document.createElement('div');
        tooltip.id = 'track-tooltip';
        tooltip.className = 'tooltip';
        document.body.appendChild(tooltip);
    }

    const pair = element.getAttribute('data-pair');
    const direction = element.getAttribute('data-direction');
    const startRank = element.getAttribute('data-start-rank');
    const endRank = element.getAttribute('data-end-rank');
    const startTime = element.getAttribute('data-start-time');
    const endTime = element.getAttribute('data-end-time');
    const pointsCount = element.getAttribute('data-points-count');
    const lastPrice = element.getAttribute('data-last-price');
    const lastChange = element.getAttribute('data-last-change');
    const lastVolume = element.getAttribute('data-last-volume');
    const lastTime = element.getAttribute('data-last-time');

    let content = `<strong>${pair}</strong><br>`;
    if (direction) {
        const dirText = direction === 'up' ? '📈 Rising' : (direction === 'down' ? '📉 Falling' : '↔ Flat');
        content += `<strong>Direction:</strong> ${dirText}<br>`;
    }
    if (startRank && endRank) {
        content += `<strong>Rank:</strong> ${startRank} → ${endRank}<br>`;
    }
    if (lastTime) {
        const lastDate = new Date(lastTime);
        const formattedTime = lastDate.toISOString().replace('T', ' ').substring(0, 19) + ' UTC';
        content += `<strong>Time (UTC0):</strong> ${formattedTime}<br>`;
    }
    if (pointsCount) {
        content += `<strong>Track points:</strong> ${pointsCount}<br>`;
    }
    if (lastPrice) {
        content += `<strong>Price:</strong> ${parseFloat(lastPrice).toFixed(8)}<br>`;
    }
    if (lastChange) {
        const change = parseFloat(lastChange);
        const changeColor = change >= 0 ? '#4CAF50' : '#F44336';
        const changeSign = change >= 0 ? '+' : '';
        content += `<strong>24h change:</strong> <span style="color: ${changeColor}">${changeSign}${change.toFixed(2)}%</span><br>`;
    }
    if (lastVolume) {
        // Format volume
        let volume = parseFloat(lastVolume);
        let volumeText;
        if (volume >= 1e9) {
            volumeText = `${(volume / 1e9).toFixed(2)}B`;
        } else if (volume >= 1e6) {
            volumeText = `${(volume / 1e6).toFixed(2)}M`;
        } else if (volume >= 1e3) {
            volumeText = `${(volume / 1e3).toFixed(2)}K`;
        } else {
            volumeText = volume.toFixed(2);
        }
        content += `<strong>24h volume:</strong> ${volumeText}`;
    }

    tooltip.innerHTML = content;
    tooltip.style.display = 'block';
    positionTooltip(tooltip, clientX, clientY);
    currentTooltip = tooltip;
}
        function positionTooltip(tooltip, x, y) {
            const offset = 15;
            const tooltipWidth = tooltip.offsetWidth;
            const tooltipHeight = tooltip.offsetHeight;
            const windowWidth = window.innerWidth;
            const windowHeight = window.innerHeight;

            let left = x + offset;
            let top = y + offset;

            if (left + tooltipWidth > windowWidth) {
                left = x - tooltipWidth - offset;
            }
            if (top + tooltipHeight > windowHeight) {
                top = y - tooltipHeight - offset;
            }

            tooltip.style.left = left + 'px';
            tooltip.style.top = top + 'px';
        }

        function hideTrackTooltip() {
            if (currentTooltip) {
                currentTooltip.style.display = 'none';
            }
        }

        function initializePanZoom(svgElement) {
            loadViewState();

            const viewport = document.createElementNS('http://www.w3.org/2000/svg', 'g');
            viewport.id = 'viewport';

            while(svgElement.firstChild) {
                viewport.appendChild(svgElement.firstChild);
            }
            svgElement.appendChild(viewport);

            const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
            rect.setAttribute('width', '100%');
            rect.setAttribute('height', '100%');
            rect.setAttribute('fill', '#000');
            viewport.appendChild(rect);

            viewport.insertBefore(rect, viewport.firstChild);

            const trackPaths = svgElement.querySelectorAll('.track-path');
            trackPaths.forEach(track => {
                track.addEventListener('mouseenter', (e) => {
                    showTrackTooltip(track, e.clientX, e.clientY);
                });
                track.addEventListener('mousemove', (e) => {
                    if (currentTooltip && currentTooltip.style.display === 'block') {
                        positionTooltip(currentTooltip, e.clientX, e.clientY);
                    }
                });
                track.addEventListener('mouseleave', hideTrackTooltip);
            });

            svgElement.addEventListener('mousedown', startPan);
            svgElement.addEventListener('mousemove', pan);
            svgElement.addEventListener('mouseup', endPan);
            svgElement.addEventListener('mouseleave', endPan);
            svgElement.addEventListener('wheel', zoom, { passive: false });

            svgElement.addEventListener('touchstart', handleTouchStart);
            svgElement.addEventListener('touchmove', handleTouchMove);
            svgElement.addEventListener('touchend', handleTouchEnd);

            createControls(svgElement);

            updateViewport();
        }

        function startPan(e) {
            if (e.target.classList.contains('track-path')) return;

            isPanning = true;
            startX = e.clientX - translateX;
            startY = e.clientY - translateY;
            e.currentTarget.style.cursor = 'grabbing';
            hideTrackTooltip();
            e.preventDefault();
        }

        function pan(e) {
            if (!isPanning) return;

            translateX = e.clientX - startX;
            translateY = e.clientY - startY;

            updateViewport();
            saveViewState();
            e.preventDefault();
        }

        function endPan(e) {
            isPanning = false;
            e.currentTarget.style.cursor = 'grab';
        }

        function zoom(e) {
            e.preventDefault();

            const zoomIntensity = 0.01;
            const wheel = e.deltaY < 0 ? 1 : -1;
            const zoomFactor = Math.exp(wheel * zoomIntensity);

            const mouseX = e.clientX;
            const mouseY = e.clientY;

            const newScale = scale * zoomFactor;
            const scaleChange = newScale - scale;

            translateX -= mouseX * (scaleChange / scale);
            translateY -= mouseY * (scaleChange / scale);

            scale = newScale;

            scale = Math.min(Math.max(0.1, scale), 10);

            updateViewport();
            saveViewState();
        }

        function updateViewport() {
            const viewport = document.getElementById('viewport');
            if (viewport) {
                viewport.setAttribute('transform', 
                    `translate(${translateX},${translateY}) scale(${scale})`);
            }
        }

        function resetView() {
            scale = 1;
            translateX = 0;
            translateY = 0;
            updateViewport();
            saveViewState();
        }

        function zoomIn() {
            scale = Math.min(10, scale * 1.5);
            updateViewport();
            saveViewState();
        }

        function zoomOut() {
            scale = Math.max(0.1, scale * 0.67);
            updateViewport();
            saveViewState();
        }

        function createControls(svgElement) {
            const container = svgElement.parentElement;

            const controls = document.createElement('div');
            controls.className = 'controls';
            controls.innerHTML = `
                <button class="control-btn" onclick="zoomIn()" title="Zoom in (x1.5)">+</button><br>
                <button class="control-btn" onclick="zoomOut()" title="Zoom out (x0.67)">-</button><br>
                <button class="control-btn" onclick="resetView()" title="Reset view">↺</button>
            `;

            container.appendChild(controls);
        }

        let initialDistance = null;
        let initialScale = 1;

        function handleTouchStart(e) {
            if (e.touches.length === 1) {
                isPanning = true;
                startX = e.touches[0].clientX - translateX;
                startY = e.touches[0].clientY - translateY;
                hideTrackTooltip();
            } else if (e.touches.length === 2) {
                const dx = e.touches[0].clientX - e.touches[1].clientX;
                const dy = e.touches[0].clientY - e.touches[1].clientY;
                initialDistance = Math.sqrt(dx * dx + dy * dy);
                initialScale = scale;
                isPanning = false;
            }
            e.preventDefault();
        }

        function handleTouchMove(e) {
            if (e.touches.length === 1 && isPanning) {
                translateX = e.touches[0].clientX - startX;
                translateY = e.touches[0].clientY - startY;
                updateViewport();
                saveViewState();
            } else if (e.touches.length === 2 && initialDistance !== null) {
                const dx = e.touches[0].clientX - e.touches[1].clientX;
                const dy = e.touches[0].clientY - e.touches[1].clientY;
                const distance = Math.sqrt(dx * dx + dy * dy);

                scale = initialScale * (distance / initialDistance);
                scale = Math.min(Math.max(0.1, scale), 10);

                updateViewport();
                saveViewState();
            }
            e.preventDefault();
        }

        function handleTouchEnd(e) {
            isPanning = false;
            initialDistance = null;
        }

        document.addEventListener('DOMContentLoaded', () => {
            setTimeout(() => {
                const svgElement = document.querySelector('svg');
                if (svgElement) {
                    svgElement.style.backgroundColor = '#000';
                    initializePanZoom(svgElement);
                }
            }, 100);
        });

        window.zoomIn = zoomIn;
        window.zoomOut = zoomOut;
        window.resetView = resetView;
        </script>
        '''

        # Add HTML with container
        html_content = f'''
        {container_style}
        <div class="svg-container" id="svg-container">
            <div class="svg-content">
                {svg_content}
            </div>
        </div>
        {js_code}
        '''

        # Display in Streamlit
        import streamlit.components.v1 as components
        components.html(html_content, height=container_height + 100, scrolling=False)

        # Statistics for filtered data
        try:
            from track_builder import TrackBuilder
            track_builder = TrackBuilder(self.storage)

            # Convert minutes to hours for loading from DB
            lookback_hours = max(1, (st.session_state.tracks_filter_minutes + 59) // 60)

            all_tracks = track_builder.load_tracks_from_db(
                exchange, market_type, lookback_hours=lookback_hours
            )

            if all_tracks:
                # Filter tracks for statistics
                filtered_tracks = self._filter_recent_tracks(all_tracks, st.session_state.tracks_filter_minutes)

                # Filter by volume
                filtered_tracks = self._filter_tracks_by_volume(filtered_tracks, st.session_state.tracks_min_volume)

                # Filter by rank change
                filtered_tracks = self._filter_tracks_by_rank_change(filtered_tracks,
                                                                     st.session_state.tracks_min_rank_change)

                total_tracks = sum(len(tracks) for tracks in filtered_tracks.values())

                up_tracks = sum(
                    sum(1 for t in tracks if t.direction == 'up')
                    for tracks in filtered_tracks.values()
                )
                down_tracks = sum(
                    sum(1 for t in tracks if t.direction == 'down')
                    for tracks in filtered_tracks.values()
                )
                flat_tracks = total_tracks - up_tracks - down_tracks

                st.markdown(f"**Track statistics:**")
                col_stats = st.columns(4)
                with col_stats[0]:
                    st.metric("Total tracks", total_tracks)
                with col_stats[1]:
                    st.metric("📈 Rising", up_tracks)
                with col_stats[2]:
                    st.metric("📉 Falling", down_tracks)
                with col_stats[3]:
                    st.metric("↔️ Flat", flat_tracks)

                # Additional statistics: rank division price in % and manual pair changes
                if filtered_tracks:
                    # 1. Calculate rank division price in %
                    # Find minimum and maximum ranks among all tracks
                    all_ranks = []
                    all_changes = []  # price changes in percent

                    for pair, track_list in filtered_tracks.items():
                        for track in track_list:
                            if track.points:
                                for point in track.points:
                                    all_ranks.append(point.rank)
                                    all_changes.append(point.change)

                    if all_ranks and all_changes:
                        # Find points with minimum and maximum ranks
                        min_rank = min(all_ranks)
                        max_rank = max(all_ranks)

                        # Find corresponding price changes
                        min_rank_changes = [ch for rk, ch in zip(all_ranks, all_changes) if rk == min_rank]
                        max_rank_changes = [ch for rk, ch in zip(all_ranks, all_changes) if rk == max_rank]

                        if min_rank_changes and max_rank_changes:
                            # Take average changes for each rank
                            avg_min_rank_change = sum(min_rank_changes) / len(min_rank_changes)
                            avg_max_rank_change = sum(max_rank_changes) / len(max_rank_changes)

                            # Calculate rank division price in %
                            if max_rank > min_rank:
                                rank_division_price = (avg_max_rank_change - avg_min_rank_change) / (
                                            max_rank - min_rank)
                                st.markdown(f"**Rank division price:** {rank_division_price:.4f} % per rank unit")
                                st.caption(
                                    f"Rank {min_rank} → {max_rank}: {avg_min_rank_change:.2f}% → {avg_max_rank_change:.2f}%")

                    # 2. Information about latest changes of manual pairs
                    manual_tracks_info = []
                    for pair, track_list in filtered_tracks.items():
                        for track in track_list:
                            if track.track_type == 'manual' and track.points:
                                last_point = track.points[-1]
                                manual_tracks_info.append({
                                    'pair': pair,
                                    'last_change': last_point.change,
                                    'last_rank': last_point.rank,
                                    'direction': track.direction,
                                    'last_time': track.end_time
                                })

                    if manual_tracks_info:
                        st.markdown("**Manual pairs (latest changes):**")

                        # Sort by time (most recent first)
                        manual_tracks_info.sort(key=lambda x: x['last_time'], reverse=True)

                        # Group by pairs to avoid duplicates
                        unique_manual_pairs = {}
                        for info in manual_tracks_info:
                            if info['pair'] not in unique_manual_pairs:
                                unique_manual_pairs[info['pair']] = info

                        # Display table
                        for pair, info in list(unique_manual_pairs.items())[:10]:  # Limit to 10 pairs
                            change_color = "#4CAF50" if info['last_change'] >= 0 else "#F44336"
                            change_sign = "+" if info['last_change'] >= 0 else ""

                            col1, col2, col3, col4 = st.columns([2, 2, 2, 2])
                            with col1:
                                st.markdown(f"**{pair}**")
                            with col2:
                                st.markdown(
                                    f"<span style='color:{change_color}'>{change_sign}{info['last_change']:.2f}%</span>",
                                    unsafe_allow_html=True)
                            with col3:
                                st.markdown(f"Rank: {info['last_rank']}")
                            with col4:
                                direction_symbol = "📈" if info['direction'] == 'up' else "📉" if info[
                                                                                                    'direction'] == 'down' else "➡️"
                                st.markdown(f"{direction_symbol} {info['direction']}")

                        if len(unique_manual_pairs) > 10:
                            st.caption(f"And {len(unique_manual_pairs) - 10} more manual pairs...")

                # 3. Top-10 rising and falling pairs
                if filtered_tracks:
                    # Collect last points of all pairs
                    last_points_info = []

                    for pair, track_list in filtered_tracks.items():
                        # For each pair take the last point from the latest track
                        if track_list:
                            # Sort tracks by end time (most recent first)
                            sorted_tracks = sorted(track_list, key=lambda x: x.end_time, reverse=True)
                            latest_track = sorted_tracks[0]

                            if latest_track.points:
                                last_point = latest_track.points[-1]
                                last_points_info.append({
                                    'pair': pair,
                                    'last_change': last_point.change,
                                    'last_rank': last_point.rank,
                                    'direction': latest_track.direction,
                                    'last_time': latest_track.end_time,
                                    'track_type': latest_track.track_type,
                                    'volume': last_point.volume
                                })

                    if last_points_info:
                        # Top-10 rising pairs (by descending change)
                        growing_pairs = sorted(
                            [p for p in last_points_info if p['last_change'] > 0],
                            key=lambda x: x['last_change'],
                            reverse=True
                        )[:10]

                        # Top-10 falling pairs (by ascending change, i.e. most negative)
                        falling_pairs = sorted(
                            [p for p in last_points_info if p['last_change'] < 0],
                            key=lambda x: x['last_change']
                        )[:10]

                        # Create two columns for display
                        col_top_grow, col_top_fall = st.columns(2)

                        with col_top_grow:
                            if growing_pairs:
                                st.markdown("**Top-10 rising pairs:**")
                                grow_data = []
                                for i, info in enumerate(growing_pairs, 1):
                                    volume_text = ""
                                    if info['volume'] >= 1000000:
                                        volume_text = f"{info['volume'] / 1000000:.1f}M"
                                    elif info['volume'] >= 1000:
                                        volume_text = f"{info['volume'] / 1000:.0f}K"
                                    else:
                                        volume_text = f"{info['volume']:.0f}"

                                    grow_data.append({
                                        "#": i,
                                        "Pair": info['pair'],
                                        "Change": f"+{info['last_change']:.2f}%",
                                        "Rank": info['last_rank'],
                                        "Volume": volume_text,
                                        "Type": "Manual" if info['track_type'] == 'manual' else "Auto"
                                    })

                                # Display as table
                                grow_df = pd.DataFrame(grow_data)
                                st.dataframe(
                                    grow_df,
                                    column_config={
                                        "#": st.column_config.NumberColumn(width="small"),
                                        "Pair": st.column_config.TextColumn(width="medium"),
                                        "Change": st.column_config.TextColumn(width="small"),
                                        "Rank": st.column_config.NumberColumn(width="small"),
                                        "Volume": st.column_config.TextColumn(width="small"),
                                        "Type": st.column_config.TextColumn(width="small")
                                    },
                                    hide_index=True,
                                    #use_container_width=True
                                    width = 'stretch'
                                )
                            else:
                                st.markdown("**Rising pairs:**")
                                st.info("No rising pairs in current selection")

                        with col_top_fall:
                            if falling_pairs:
                                st.markdown("**Top-10 falling pairs:**")
                                fall_data = []
                                for i, info in enumerate(falling_pairs, 1):
                                    volume_text = ""
                                    if info['volume'] >= 1000000:
                                        volume_text = f"{info['volume'] / 1000000:.1f}M"
                                    elif info['volume'] >= 1000:
                                        volume_text = f"{info['volume'] / 1000:.0f}K"
                                    else:
                                        volume_text = f"{info['volume']:.0f}"

                                    fall_data.append({
                                        "#": i,
                                        "Pair": info['pair'],
                                        "Change": f"{info['last_change']:.2f}%",
                                        "Rank": info['last_rank'],
                                        "Volume": volume_text,
                                        "Type": "Manual" if info['track_type'] == 'manual' else "Auto"
                                    })

                                # Display as table
                                fall_df = pd.DataFrame(fall_data)
                                st.dataframe(
                                    fall_df,
                                    column_config={
                                        "#": st.column_config.NumberColumn(width="small"),
                                        "Pair": st.column_config.TextColumn(width="medium"),
                                        "Change": st.column_config.TextColumn(width="small"),
                                        "Rank": st.column_config.NumberColumn(width="small"),
                                        "Volume": st.column_config.TextColumn(width="small"),
                                        "Type": st.column_config.TextColumn(width="small")
                                    },
                                    hide_index=True,
                                    #use_container_width=True
                                    width = 'stretch'
                                )
                            else:
                                st.markdown("**Falling pairs:**")
                                st.info("No falling pairs in current selection")

                        # Additional distribution info
                        total_pairs = len(last_points_info)
                        growing_count = len([p for p in last_points_info if p['last_change'] > 0])
                        falling_count = len([p for p in last_points_info if p['last_change'] < 0])
                        neutral_count = total_pairs - growing_count - falling_count

                        st.caption(
                            f"📊 Distribution: 📈 {growing_count} rising | 📉 {falling_count} falling | ➡️ {neutral_count} flat")

                        # Average values
                        if growing_pairs:
                            avg_growth = sum(p['last_change'] for p in growing_pairs) / len(growing_pairs)
                            st.caption(f"Average rise in top-10: +{avg_growth:.2f}%")

                        if falling_pairs:
                            avg_fall = sum(p['last_change'] for p in falling_pairs) / len(falling_pairs)
                            st.caption(f"Average fall in top-10: {avg_fall:.2f}%")

                # 4. Top-10 rising and falling over the display period (change over period)
                if filtered_tracks:
                    # For period change we need earliest and latest points within the period
                    # We will search for each pair the earliest and latest point within the filter
                    period_changes_info = []

                    # Get display period from session state
                    period_minutes = st.session_state.tracks_filter_minutes
                    from datetime import timezone

                    # Calculate period start time
                    period_start_time = datetime.now(timezone.utc) - timedelta(minutes=period_minutes)

                    for pair, track_list in filtered_tracks.items():
                        # Collect all points for this pair within the period
                        all_points_in_period = []

                        for track in track_list:
                            # Filter points by time (only those in period)
                            points_in_track = [p for p in track.points if p.time >= period_start_time]
                            all_points_in_period.extend(points_in_track)

                        if len(all_points_in_period) >= 2:
                            # Sort points by time
                            sorted_points = sorted(all_points_in_period, key=lambda x: x.time)

                            # Take earliest and latest point
                            earliest_point = sorted_points[0]
                            latest_point = sorted_points[-1]

                            # Calculate period change in percent
                            if earliest_point.price > 0:
                                period_change_percent = ((latest_point.price - earliest_point.price) / earliest_point.price) * 100

                                # Calculate rank change over period
                                rank_change = latest_point.rank - earliest_point.rank

                                # Determine direction over period
                                if period_change_percent > 1:  # Rise more than 1%
                                    period_direction = 'up'
                                elif period_change_percent < -1:  # Fall more than 1%
                                    period_direction = 'down'
                                else:
                                    period_direction = 'flat'

                                # Average volume over period (from all points)
                                avg_volume = sum(p.volume for p in sorted_points) / len(sorted_points)

                                period_changes_info.append({
                                    'pair': pair,
                                    'period_change': period_change_percent,
                                    'start_price': earliest_point.price,
                                    'end_price': latest_point.price,
                                    'start_rank': earliest_point.rank,
                                    'end_rank': latest_point.rank,
                                    'rank_change': rank_change,
                                    'period_direction': period_direction,
                                    'avg_volume': avg_volume,
                                    'point_count': len(sorted_points),
                                    'start_time': earliest_point.time,
                                    'end_time': latest_point.time
                                })

                    if period_changes_info:
                        st.markdown("---")
                        st.subheader(f"📊 Changes over period ({period_minutes} minutes)")

                        # Top-10 rising over period (by descending change)
                        period_growing_pairs = sorted(
                            [p for p in period_changes_info if p['period_change'] > 0],
                            key=lambda x: x['period_change'],
                            reverse=True
                        )[:10]

                        # Top-10 falling over period (by ascending change)
                        period_falling_pairs = sorted(
                            [p for p in period_changes_info if p['period_change'] < 0],
                            key=lambda x: x['period_change']
                        )[:10]

                        # Create two columns
                        col_period_grow, col_period_fall = st.columns(2)

                        with col_period_grow:
                            if period_growing_pairs:
                                st.markdown(f"**Top-10 rising over {period_minutes} min:**")
                                grow_period_data = []
                                for i, info in enumerate(period_growing_pairs, 1):
                                    # Format volume
                                    volume_text = ""
                                    if info['avg_volume'] >= 1000000:
                                        volume_text = f"{info['avg_volume'] / 1000000:.1f}M"
                                    elif info['avg_volume'] >= 1000:
                                        volume_text = f"{info['avg_volume'] / 1000:.0f}K"
                                    else:
                                        volume_text = f"{info['avg_volume']:.0f}"

                                    # Format prices
                                    start_price_text = f"{info['start_price']:.8f}"
                                    end_price_text = f"{info['end_price']:.8f}"

                                    grow_period_data.append({
                                        "#": i,
                                        "Pair": info['pair'],
                                        "Change": f"+{info['period_change']:.2f}%",
                                        "Rank": f"{info['start_rank']}→{info['end_rank']}",
                                        "Price": f"{start_price_text}→{end_price_text}",
                                        "Volume": volume_text,
                                        "Points": info['point_count']
                                    })

                                # Display table
                                grow_period_df = pd.DataFrame(grow_period_data)
                                st.dataframe(
                                    grow_period_df,
                                    column_config={
                                        "#": st.column_config.NumberColumn(width="small"),
                                        "Pair": st.column_config.TextColumn(width="medium"),
                                        "Change": st.column_config.TextColumn(width="small"),
                                        "Rank": st.column_config.TextColumn(width="small"),
                                        "Price": st.column_config.TextColumn(width="medium"),
                                        "Volume": st.column_config.TextColumn(width="small"),
                                        "Points": st.column_config.NumberColumn(width="small")
                                    },
                                    hide_index=True,
                                    #use_container_width=True
                                    width='stretch'
                                )

                                # Statistics for top-10 rising
                                if period_growing_pairs:
                                    avg_growth_period = sum(p['period_change'] for p in period_growing_pairs) / len(
                                        period_growing_pairs)
                                    max_growth_period = max(p['period_change'] for p in period_growing_pairs)
                                    st.caption(
                                        f"📈 Average rise: +{avg_growth_period:.2f}%, Max: +{max_growth_period:.2f}%")
                            else:
                                st.markdown(f"**Rising over {period_minutes} min:**")
                                st.info(f"No rising pairs over {period_minutes} minutes")

                        with col_period_fall:
                            if period_falling_pairs:
                                st.markdown(f"**Top-10 falling over {period_minutes} min:**")
                                fall_period_data = []
                                for i, info in enumerate(period_falling_pairs, 1):
                                    # Format volume
                                    volume_text = ""
                                    if info['avg_volume'] >= 1000000:
                                        volume_text = f"{info['avg_volume'] / 1000000:.1f}M"
                                    elif info['avg_volume'] >= 1000:
                                        volume_text = f"{info['avg_volume'] / 1000:.0f}K"
                                    else:
                                        volume_text = f"{info['avg_volume']:.0f}"

                                    # Format prices
                                    start_price_text = f"{info['start_price']:.8f}"
                                    end_price_text = f"{info['end_price']:.8f}"

                                    fall_period_data.append({
                                        "#": i,
                                        "Pair": info['pair'],
                                        "Change": f"{info['period_change']:.2f}%",
                                        "Rank": f"{info['start_rank']}→{info['end_rank']}",
                                        "Price": f"{start_price_text}→{end_price_text}",
                                        "Volume": volume_text,
                                        "Points": info['point_count']
                                    })

                                # Display table
                                fall_period_df = pd.DataFrame(fall_period_data)
                                st.dataframe(
                                    fall_period_df,
                                    column_config={
                                        "#": st.column_config.NumberColumn(width="small"),
                                        "Pair": st.column_config.TextColumn(width="medium"),
                                        "Change": st.column_config.TextColumn(width="small"),
                                        "Rank": st.column_config.TextColumn(width="small"),
                                        "Price": st.column_config.TextColumn(width="medium"),
                                        "Volume": st.column_config.TextColumn(width="small"),
                                        "Points": st.column_config.NumberColumn(width="small")
                                    },
                                    hide_index=True,
                                    #use_container_width=True
                                    width = 'stretch'
                                )

                                # Statistics for top-10 falling
                                if period_falling_pairs:
                                    avg_fall_period = sum(p['period_change'] for p in period_falling_pairs) / len(
                                        period_falling_pairs)
                                    max_fall_period = min(
                                        p['period_change'] for p in period_falling_pairs)  # Most negative
                                    st.caption(
                                        f"📉 Average fall: {avg_fall_period:.2f}%, Max: {max_fall_period:.2f}%")
                            else:
                                st.markdown(f"**Falling over {period_minutes} min:**")
                                st.info(f"No falling pairs over {period_minutes} minutes")

                        # Overall period statistics
                        st.markdown("**Overall period statistics:**")

                        # Calculate total metrics
                        total_period_pairs = len(period_changes_info)
                        period_growing_count = len([p for p in period_changes_info if p['period_change'] > 1])
                        period_falling_count = len([p for p in period_changes_info if p['period_change'] < -1])
                        period_neutral_count = total_period_pairs - period_growing_count - period_falling_count

                        # Average change over all pairs in period
                        if period_changes_info:
                            avg_period_change = sum(p['period_change'] for p in period_changes_info) / len(
                                period_changes_info)

                            # Median change
                            median_period_change = sorted([p['period_change'] for p in period_changes_info])[
                                len(period_changes_info) // 2]

                            # Maximum and minimum change
                            max_period_change = max(p['period_change'] for p in period_changes_info)
                            min_period_change = min(p['period_change'] for p in period_changes_info)

                            # Columns for statistics
                            col_stats1, col_stats2, col_stats3, col_stats4 = st.columns(4)

                            with col_stats1:
                                st.metric("Total pairs", total_period_pairs)
                            with col_stats2:
                                st.metric("📈 Rise (>1%)", period_growing_count)
                            with col_stats3:
                                st.metric("📉 Fall (<-1%)", period_falling_count)
                            with col_stats4:
                                st.metric("➡️ Neutral", period_neutral_count)

                            # Additional metrics
                            col_metrics1, col_metrics2, col_metrics3 = st.columns(3)

                            with col_metrics1:
                                st.metric("Average change", f"{avg_period_change:.2f}%")
                            with col_metrics2:
                                st.metric("Median change", f"{median_period_change:.2f}%")
                            with col_metrics3:
                                st.metric("Range", f"{min_period_change:.2f}% / {max_period_change:.2f}%")

                            # Period time range
                            if period_changes_info:
                                earliest_period_time = min(p['start_time'] for p in period_changes_info)
                                latest_period_time = max(p['end_time'] for p in period_changes_info)

                                st.caption(
                                    f"⏱️ Period: {earliest_period_time.strftime('%H:%M:%S')} - {latest_period_time.strftime('%H:%M:%S')}")

                # Filter info
                cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=st.session_state.tracks_filter_minutes)
                current_time = datetime.now(timezone.utc)
                filter_info = f"Showing tracks from {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')} to {current_time.strftime('%Y-%m-%d %H:%M:%S')}"

                if st.session_state.tracks_min_volume > 0:
                    volume_text = f"{st.session_state.tracks_min_volume / 1000000:.2f}M" if st.session_state.tracks_min_volume >= 1000000 else \
                        f"{st.session_state.tracks_min_volume / 1000:.0f}K" if st.session_state.tracks_min_volume >= 1000 else \
                            f"{st.session_state.tracks_min_volume:.0f}"
                    filter_info += f" | Min volume: {volume_text}"

                if st.session_state.tracks_min_rank_change > 0:
                    filter_info += f" | Min rank change: {st.session_state.tracks_min_rank_change}"

                st.caption(filter_info)
        except Exception as e:
            st.error(f"Error loading statistics: {e}")