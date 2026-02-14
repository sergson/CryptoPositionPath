# config_page.py (fixed version)
"""
Configuration page for parameters
"""
import streamlit as st
from data_storage import DataStorage
from data_collector import DataCollector
from analytics_engine import AnalyticsEngine
from logger import perf_logger
import sqlite3
import pandas as pd
from typing import List


class ConfigPage:
    """Configuration page"""

    def __init__(self):
        self.storage = DataStorage()
        self.analytics = AnalyticsEngine(self.storage)  # Added AnalyticsEngine
        self._init_session_state()
        self._load_settings()
        self.logger = perf_logger.get_logger('config_page', 'config')
        self._init_threshold_tracking()

    def _init_threshold_tracking(self):
        """Initialize tracking of threshold changes"""
        if 'previous_threshold' not in st.session_state:
            current_threshold = self.storage.get_setting('rank_threshold', 20)
            st.session_state.previous_threshold = current_threshold
        if 'threshold_changed' not in st.session_state:
            st.session_state.threshold_changed = False

    def _check_threshold_change(self):
        """Check if sensitivity threshold has changed"""
        current_threshold = self.storage.get_setting('rank_threshold', 20)
        previous_threshold = st.session_state.get('previous_threshold', current_threshold)

        if current_threshold != previous_threshold:
            st.session_state.threshold_changed = True
            st.session_state.previous_threshold = current_threshold
            self.logger.debug(f"📊 Sensitivity threshold changed: {previous_threshold} -> {current_threshold}")
        else:
            st.session_state.threshold_changed = False

    def _init_session_state(self):
        """Initialize session state with default values"""
        defaults = {
            "exchange": "binance",
            "quote_currency": "USDT",
            "markets": ["spot"],
            "interval": 60,
            "retention": 24,
            "pair_limit": 800,
            "manual_pairs": [],
            "loaded_settings": False,
            "debug_view": None,  # 'colors' or 'tables'
            "colors_page": 1,
            "tables_page": 1,
            "colors_search": "",
            "tables_search": ""
        }

        for key, default_value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = default_value

    def _load_settings(self):
        """Load saved settings on startup"""
        if not st.session_state.get("loaded_settings", False):
            st.session_state.loaded_settings = True

            # Load settings from database
            settings = self.storage.get_all_settings()

            # Ensure settings exist in database
            for key in ['exchange', 'quote_currency', 'markets', 'interval',
                        'retention', 'pair_limit', 'manual_pairs']:
                if key in settings:
                    value = settings[key]
                    # Make sure value is correct
                    if value is not None:
                        st.session_state[key] = value
                    else:
                        # If value is None, use default
                        default_values = {
                            "exchange": "binance",
                            "quote_currency": "USDT",
                            "markets": ["spot"],
                            "interval": 60,
                            "retention": 24,
                            "pair_limit": 800,
                            "manual_pairs": []
                        }
                        if key in default_values:
                            st.session_state[key] = default_values[key]

    def _save_setting_on_change(self, key: str, value: any):
        """Save setting on change"""
        st.session_state[key] = value
        self.storage.save_setting(key, value)

    def _get_current_interval_seconds(self) -> int:
        """Get current interval from settings"""
        try:
            interval_setting = self.storage.get_setting('interval', 60)
            current_interval = st.session_state.get("interval", 60)
            if current_interval != interval_setting:
                self._save_setting_on_change('interval', current_interval)
                interval_setting = current_interval
            if isinstance(interval_setting, str):
                return int(interval_setting)
            return interval_setting
        except Exception as e:
            self.logger.error(f"Error getting current interval from settings, returning: 1 min: {e}")
            return 60


    def display(self):
        """Display configuration page"""

        # Ensure logger is initialized with DB settings
        if not hasattr(perf_logger, '_storage_initialized'):
            perf_logger.initialize_with_storage(self.storage)
            perf_logger._storage_initialized = True

        st.title("⚙️ Data Collection Configuration")
        st.markdown("---")

        # Settings container
        with st.container():
            col1, col2 = st.columns(2)

            with col1:
                # Exchange selection
                exchanges = {
                    "Binance": "binance",
                    "MEXC": "mexc",
                    "Bybit": "bybit",
                    "Gate.io": "gate",
                    "KuCoin": "kucoin",
                    "OKX": "okx"
                }

                exchange_display_names = list(exchanges.keys())
                exchange_codes = list(exchanges.values())

                current_exchange = st.session_state.get("exchange", "binance")
                current_index = exchange_codes.index(current_exchange) if current_exchange in exchange_codes else 0

                selected_exchange_display = st.selectbox(
                    "**Exchange**",
                    exchange_display_names,
                    index=current_index,
                    key="exchange_select"
                )
                exchange_code = exchanges[selected_exchange_display]
                st.session_state.exchange = exchange_code

                # Quote currency filter
                quote_currencies = ["BTC", "USDT", "USDC", "ETH", "All pairs"]
                current_currency = st.session_state.get("quote_currency", "USDT")
                currency_index = quote_currencies.index(current_currency) if current_currency in quote_currencies else 1

                selected_currency = st.selectbox(
                    "**Quote Currency**",
                    quote_currencies,
                    index=currency_index,
                    key="quote_currency_select"
                )
                st.session_state.quote_currency = selected_currency

            with col2:
                # Request interval
                interval_options = {
                    "1 minute": 60,
                    "5 minutes": 300,
                    "15 minutes": 900,
                }

                interval_display_names = list(interval_options.keys())
                interval_values = list(interval_options.values())

                current_interval = st.session_state.get("interval", 60)
                current_index = interval_values.index(current_interval) if current_interval in interval_values else 2

                selected_interval_display = st.selectbox(
                    "**Request Interval**",
                    interval_display_names,
                    index=current_index,
                    key="interval_select"
                )
                interval_seconds = interval_options[selected_interval_display]
                st.session_state.interval = interval_seconds

                # Retention period
                retention_options = {
                    "1 hour": 1,
                    "4 hours": 4,
                    "1 day": 24
                }

                retention_display_names = list(retention_options.keys())
                retention_values = list(retention_options.values())

                current_retention = st.session_state.get("retention", 24)
                current_index = retention_values.index(current_retention) if current_retention in retention_values else 4

                selected_retention_display = st.selectbox(
                    "**Retention Period**",
                    retention_display_names,
                    index=current_index,
                    key="retention_select"
                )
                retention_hours = retention_options[selected_retention_display]
                st.session_state.retention = retention_hours

        # Second row of settings
        col1, col2 = st.columns(2)

        with col1:
            # Market selection
            market_options = ["Spot", "Futures"]
            current_markets = st.session_state.get("markets", ["spot"])
            current_markets_display = [m.capitalize() for m in current_markets]

            selected_markets = st.multiselect(
                "**Markets**",
                market_options,
                default=[m for m in market_options if m in current_markets_display],
                key="markets_select"
            )
            markets_lower = [m.lower() for m in selected_markets]
            st.session_state.markets = markets_lower

            # Manual pair highlighting
            if exchange_code and markets_lower:
                available_pairs = self.storage.get_used_pairs(
                    exchange_code,
                    markets_lower[0] if markets_lower else "spot",
                    selected_currency if selected_currency != "All pairs" else None
                )

                if available_pairs:
                    current_manual_pairs = st.session_state.get("manual_pairs", [])
                    manual_pairs = st.multiselect(
                        "**Manual Pair Highlighting**",
                        available_pairs,
                        default=[p for p in current_manual_pairs if p in available_pairs],
                        help="Select pairs to highlight in tracks regardless of analysis",
                        key="manual_pairs_select"
                    )

                    # Save with color update
                    if manual_pairs != current_manual_pairs:
                        self._save_manual_pairs_and_update_colors(manual_pairs)
                        st.session_state.manual_pairs = manual_pairs

        with col2:
            # Number of pairs for analysis
            current_pair_limit = st.session_state.get("pair_limit", 800)
            pair_limit = st.slider(
                "**Number of Pairs for Analysis**",
                min_value=10,
                max_value=5000,
                value=current_pair_limit,
                step=10,
                key="pair_limit_slider"
            )
            st.session_state.pair_limit = pair_limit

            # Rank sensitivity threshold
            current_threshold = self.storage.get_setting('rank_threshold', 20)
            rank_threshold = st.slider(
                "**Rank Sensitivity Threshold**",
                min_value=0,
                max_value=100,
                value=current_threshold,
                step=1,
                help="Minimum rank change between snapshots to create a track (always 0 for manual pairs)"
            )
            if rank_threshold != current_threshold:
                self.storage.save_setting('rank_threshold', rank_threshold)

        st.markdown("---")

        col1, col2 = st.columns(2)
        with col1:
            # Check threshold change and show corresponding button
            self._check_threshold_change()
            if st.session_state.threshold_changed:
                st.warning("⚠️ Sensitivity threshold changed! It is recommended to rebuild tracks.")

                # Get current interval
                current_interval = self._get_current_interval_seconds()

                # Display interval info
                interval_display = f"{current_interval} sec"
                if current_interval == 60:
                    interval_display = "1 minute"
                elif current_interval == 300:
                    interval_display = "5 minutes"
                elif current_interval == 900:
                    interval_display = "15 minutes"

                st.info(f"📊 Tracks will be built with snapshot interval: **{interval_display}**")

                if st.button("🔄 **Rebuild All Tracks**",
                             type="primary",
                             help=f"Rebuild all tracks with new sensitivity threshold and interval {interval_display}",
                             key="rebuild_all_tracks"):

                    exchange = st.session_state.get("exchange", "binance")
                    markets = st.session_state.get("markets", ["spot"])

                    settings_to_save = {
                        "exchange": exchange_code,
                        "quote_currency": selected_currency,
                        "markets": markets_lower,
                        "interval": interval_seconds,
                        "retention": retention_hours,
                        "pair_limit": pair_limit,
                        "manual_pairs": st.session_state.get("manual_pairs", []),
                        'rank_threshold': rank_threshold,
                    }

                    for key, value in settings_to_save.items():
                        self.storage.save_setting(key, value)

                    with st.spinner(f"Rebuilding tracks for {exchange} with interval {interval_display}..."):
                        try:
                            self.analytics.rebuild_all_tracks(exchange, markets, current_interval)
                            st.success(f"✅ Tracks successfully rebuilt with new threshold and interval {interval_display}!")
                            st.session_state.threshold_changed = False
                        except Exception as e:
                            st.error(f"❌ Error rebuilding tracks: {e}")

            # Force rebuild all tracks button (always available)
            if st.button("🔄 **Rebuild Tracks**",
                         type="secondary",
                         help="Rebuild all tracks",
                         key="force_rebuild_all_tracks"):

                exchange = st.session_state.get("exchange", "binance")
                markets = st.session_state.get("markets", ["spot"])
                current_interval = self._get_current_interval_seconds()

                # Display interval info
                interval_display = f"{current_interval} sec"
                if current_interval == 60:
                    interval_display = "1 minute"
                elif current_interval == 300:
                    interval_display = "5 minutes"
                elif current_interval == 900:
                    interval_display = "15 minutes"

                st.info(f"📊 Tracks will be built with snapshot interval: **{interval_display}**")

                with st.spinner(f"Force rebuilding all tracks for {exchange} with interval {interval_display}..."):
                    try:
                        self.analytics.rebuild_all_tracks(exchange, markets, current_interval)
                        st.success(f"✅ All tracks successfully rebuilt with interval {interval_display}!")
                        st.session_state.threshold_changed = False
                    except Exception as e:
                        st.error(f"❌ Error rebuilding tracks: {e}")
        with col2:
            # Manual save settings button
            if st.button("💾 Save Current Settings", type="secondary"):
                # Explicitly save all settings
                settings_to_save = {
                    "exchange": exchange_code,
                    "quote_currency": selected_currency,
                    "markets": markets_lower,
                    "interval": interval_seconds,
                    "retention": retention_hours,
                    "pair_limit": pair_limit,
                    "manual_pairs": st.session_state.get("manual_pairs", []),
                    'rank_threshold': rank_threshold,
                }
                for key, value in settings_to_save.items():
                    self.storage.save_setting(key, value)

                st.success("Settings saved!")

        # Cleanup options
        with st.expander("⚡ Additional Options"):
            col1, col2 = st.columns(2)

            with col1:
                clear_db = st.checkbox(
                    "**Clear database before start**",
                    value=False,
                    help="Will delete all data snapshots but keep settings"
                )

                # Check threshold change
                self._check_threshold_change()

            with col2:
                clear_colors = st.checkbox(
                    "**Clear pair colors**",
                    value=True,
                    help="Delete all pair colors when clearing database"
                )

        st.markdown("---")

        # Control buttons
        col1, col2, col3, col4 = st.columns([1, 1, 1, 3])

        with col1:
            if st.button("🚀 **Start**", type="primary", use_container_width=True):
                # Save all settings
                settings_to_save = {
                    "exchange": exchange_code,
                    "quote_currency": selected_currency if selected_currency != "All pairs" else None,
                    "markets": markets_lower,
                    "interval": interval_seconds,
                    "retention": retention_hours,
                    "pair_limit": pair_limit,
                    "manual_pairs": st.session_state.get("manual_pairs", [])
                }

                for key, value in settings_to_save.items():
                    self.storage.save_setting(key, value)

                # Save selected manual highlighting pairs
                self.storage.save_manual_pairs(st.session_state.get("manual_pairs", []))

                # Clear database if needed
                if clear_db:
                    st.info("🧹 Clearing database...")
                    self.storage.clear_all_data(
                        keep_colors=not clear_colors,
                        keep_settings=True
                    )
                    st.success("✅ Database cleared")

                # Save settings to session state
                st.session_state.config = settings_to_save

                # Start data collection
                collector = DataCollector(self.storage)
                collector.start(
                    exchange=exchange_code,
                    market_type=markets_lower[0] if markets_lower else "spot",
                    quote_currency=selected_currency if selected_currency != "All pairs" else None,
                    interval_seconds=interval_seconds,
                    pair_limit=pair_limit,
                    retention_hours=retention_hours
                )
                st.session_state.collector = collector
                st.success("✅ Data collection started!")

        with col2:
            if st.button("📈 **Tracks**", use_container_width=True):
                # Save settings
                settings_to_save = {
                    "exchange": exchange_code,
                    "quote_currency": selected_currency if selected_currency != "All pairs" else None,
                    "markets": markets_lower,
                    "interval": interval_seconds,
                    "retention": retention_hours,
                    "pair_limit": pair_limit,
                    "manual_pairs": st.session_state.get("manual_pairs", []),
                    'rank_threshold': rank_threshold,
                }

                for key, value in settings_to_save.items():
                    self.storage.save_setting(key, value)

                st.session_state.config = {
                    "exchange": exchange_code,
                    "markets": markets_lower,
                    "pair_limit": pair_limit
                }

                # Navigate to tracks page
                st.session_state.page = "tracks"
                st.rerun()

        with col3:
            if st.button("⏹️ **Stop**", use_container_width=True):
                # Stop collector if running
                if "collector" in st.session_state:
                    st.session_state.collector.stop()
                    del st.session_state.collector
                    st.success("⏹️ Data collection stopped")
                else:
                    st.info("Data collection not running")

        with col4:
            # Data collection info
            markets_for_count = markets_lower if markets_lower else ["spot"]
            snapshot_count = self.storage.get_snapshot_count(
                exchange_code,
                markets_for_count[0]
            )

            # Check actual collection state
            is_collecting = False
            if "collector" in st.session_state:
                try:
                    # Check if thread is active
                    if hasattr(st.session_state.collector, 'is_running'):
                        is_collecting = st.session_state.collector.is_running
                    # Additional check for thread state
                    if hasattr(st.session_state.collector, 'thread'):
                        if st.session_state.collector.thread:
                            is_collecting = st.session_state.collector.thread.is_alive()
                except:
                    is_collecting = False

            status_color = "🟢" if is_collecting else "⚪"
            status_text = "Collection active" if is_collecting else "Collection inactive"

            # Get display names
            exchange_name = selected_exchange_display
            currency_name = selected_currency
            interval_name = selected_interval_display
            retention_name = selected_retention_display

            st.info(f"""
            **Status:** {status_color} {status_text}
            - Snapshots in DB: {snapshot_count}
            - Exchange: {exchange_name}
            - Quote currency: {currency_name}
            - Manual highlighting: {len(st.session_state.get('manual_pairs', []))} pairs
            - Interval: {interval_name}
            - Retention: {retention_name}
            """)

        # Debug information
        with st.expander("🔧 Debug"):
            # Get current settings
            current_settings = perf_logger.settings

            st.subheader("📝 Logging Settings")
            col1, col2 = st.columns(2)
            with col1:

                render_level = st.selectbox(
                    "Render Level",
                    ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                    index=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"].index(
                        current_settings.get('render_level', 'INFO')
                    )
                )
                perf_logger.settings['render_level'] = render_level

                db_level = st.selectbox(
                    "Database Level",
                    ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                    index=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"].index(
                        current_settings.get('db_level', 'INFO')
                    )
                )
                perf_logger.settings['db_level'] = db_level

                analytics_level = st.selectbox(
                    "Analytics Level",
                    ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                    index=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"].index(
                        current_settings.get('analytics_level', 'INFO')
                    )
                )
                perf_logger.settings['analytics_level'] = analytics_level

                collector_level = st.selectbox(
                    "Collector Level",
                    ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                    index=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"].index(
                        current_settings.get('collector_level', 'INFO')
                    )
                )
                perf_logger.settings['collector_level'] = collector_level

            with col2:
                config_level = st.selectbox(
                    "Config Level",
                    ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                    index=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"].index(
                        current_settings.get('config_level', 'INFO')
                    )
                )
                perf_logger.settings['config_level'] = config_level

                fetcher_level = st.selectbox(
                    "Fetcher Level",
                    ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                    index=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"].index(
                        current_settings.get('fetcher_level', 'INFO')
                    )
                )
                perf_logger.settings['fetcher_level'] = fetcher_level

                performance_log = st.checkbox(
                    "Performance Logging",
                    value=current_settings.get('performance_log', True)
                )
                perf_logger.settings['performance_log'] = performance_log

                if st.button("💾 Save Logging Settings"):
                    perf_logger.save_settings(self.storage)
                    st.success("Logging settings saved!")

                if st.button("📊 View Logs", key="view_logs"):
                    st.session_state.page = "logs"
                    st.rerun()

            col1, col2 = st.columns(2)
            with col1:
                if st.button("Clear Database", key="clear_db_debug"):
                    self.storage.clear_all_data(keep_colors=True, keep_settings=True)
                    st.success("Database cleared (settings kept)")

                if st.button("Clear Tracks", key="clear_tracks"):
                    self.storage.clear_tracks_table()
                    st.success("Tracks table cleared and recreated")

                if st.button("Check DB", key="check_db"):
                    self.storage.verify_db_integrity()
                    st.success("Database checked")

            with col2:
                if st.button("Clear Old Snapshots"):
                    self.storage.cleanup_old_data(retention_hours)
                    st.success(f"Snapshots older than {retention_hours} hours cleared")

                # Toggle to display colors
                if st.button("🎨 Show Pair Colors"):
                    st.session_state.debug_view = 'colors'
                    st.session_state.colors_page = 1  # Reset to first page

                # Toggle to display tables
                if st.button("📋 Show DB Tables"):
                    st.session_state.debug_view = 'tables'
                    st.session_state.tables_page = 1  # Reset to first page


            # Display colors or tables based on selection
            if st.session_state.debug_view == 'colors':
                self._display_colors_compact()
            elif st.session_state.debug_view == 'tables':
                self._display_tables_compact()


    def _save_manual_pairs_and_update_colors(self, manual_pairs: List[str]):
        """Save manual pairs and update colors"""
        # Get current and new lists
        current_manual_pairs = set(st.session_state.get("manual_pairs", []))
        new_manual_pairs = set(manual_pairs)

        # Pairs that were removed
        removed_pairs = current_manual_pairs - new_manual_pairs

        # Pairs that were added
        added_pairs = new_manual_pairs - current_manual_pairs

        # Save setting
        self._save_setting_on_change('manual_pairs', manual_pairs)

        # Get current settings
        exchange = st.session_state.get("exchange", "binance")
        market_type = st.session_state.get("markets", ["spot"])[0]

        # Delete tracks for removed pairs
        if removed_pairs:
            try:
                from manual_tracks_manager import ManualTracksManager
                manager = ManualTracksManager(self.storage)

                for pair in removed_pairs:
                    manager.remove_manual_tracks(pair, exchange, market_type)

                self.logger.info(f"✅ Deleted tracks for {len(removed_pairs)} pairs")
            except Exception as e:
                self.logger.error(f"Error deleting tracks: {e}")

        # Add colors and build tracks for new pairs
        if added_pairs:
            # Get latest snapshot to update manual_colour
            snapshots = self.storage.get_latest_snapshots(exchange, market_type, limit=1)

            if snapshots:
                latest_table_name, _, _ = snapshots[0]

                updated_count = 0
                for pair in added_pairs:
                    try:
                        # Get or create color
                        result = self.storage.get_or_create_pair_color(pair)

                        if isinstance(result, tuple) and len(result) == 2:
                            color_id, color_hex = result
                            if color_hex:
                                # Update manual_colour in snapshot
                                conn = sqlite3.connect(self.storage.db_path)
                                cursor = conn.cursor()
                                cursor.execute(f'''
                                    UPDATE {latest_table_name} 
                                    SET manual_colour = ? 
                                    WHERE pair = ?
                                ''', (color_hex, pair))
                                conn.commit()
                                conn.close()
                                updated_count += 1
                    except Exception as e:
                        self.logger.warning(f"Error updating color for {pair}: {e}")

                self.logger.info(f"✅ Updated manual colors for {updated_count} pairs")

            # Build tracks for new manual pairs
            #self._build_tracks_for_manual_pairs(list(added_pairs), exchange, market_type)

    def _build_tracks_for_manual_pairs(self, manual_pairs: List[str], exchange: str, market_type: str):
        """Build tracks for manual pairs"""
        if not manual_pairs:
            return

        try:
            from track_builder import TrackBuilder
            track_builder = TrackBuilder(self.storage)

            # Build tracks for manual pairs
            all_tracks = {}
            for pair in manual_pairs:
                tracks = track_builder.build_tracks_for_pair(
                    pair, exchange, market_type, lookback_hours=24
                )
                if tracks:
                    all_tracks[pair] = tracks

            # Save to DB
            if all_tracks:
                track_builder.save_tracks_to_db(all_tracks, exchange, market_type)
                self.logger.info(f"✅ Built tracks for {len(all_tracks)} manual pairs")

        except Exception as e:
            self.logger.error(f"Error building tracks for manual pairs: {e}")

    def _display_colors_compact(self):
        """Compact display of pair colors with state preservation"""
        # Button to hide view
        if st.button("❌ Hide Colors", key="hide_colors"):
            st.session_state.debug_view = None
            st.rerun()

        conn = sqlite3.connect(self.storage.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute('SELECT pair, color FROM pair_colors WHERE is_system = 0 ORDER BY pair')
            all_colors = cursor.fetchall()

            if not all_colors:
                st.info("No pair colors found")
                return

            st.write(f"Total pair colors: {len(all_colors)}")

            # Search by pair
            search_key = "colors_search_main"
            if search_key not in st.session_state:
                st.session_state[search_key] = ""

            search_term = st.text_input(
                "🔍 Search by pair:",
                value=st.session_state[search_key],
                key="colors_search_input_main"
            )
            st.session_state[search_key] = search_term

            # Filter by search term
            filtered_colors = all_colors
            if search_term:
                filtered_colors = [c for c in all_colors if search_term.lower() in c[0].lower()]
                st.write(f"Found: {len(filtered_colors)} pairs")

            # Pagination
            page_size = 30
            total_pages = max(1, (len(filtered_colors) + page_size - 1) // page_size)

            # Ensure current page is within limits
            current_page = st.session_state.colors_page
            if current_page > total_pages:
                current_page = total_pages
                st.session_state.colors_page = current_page

            # Display pagination controls
            col1, col2, col3 = st.columns([2, 3, 2])
            with col1:
                if st.button("⏮️ First", key="colors_first"):
                    st.session_state.colors_page = 1
                    st.rerun()
                if st.button("◀️ Prev", key="colors_prev") and current_page > 1:
                    st.session_state.colors_page = current_page - 1
                    st.rerun()

            with col2:
                page = st.number_input(
                    "Page:",
                    min_value=1,
                    max_value=total_pages,
                    value=current_page,
                    key="colors_page_input_main",
                    on_change=lambda: setattr(st.session_state, 'colors_page',
                                              st.session_state.colors_page_input_main)
                )
                st.session_state.colors_page = page

            with col3:
                if st.button("Next ▶️", key="colors_next") and current_page < total_pages:
                    st.session_state.colors_page = current_page + 1
                    st.rerun()
                if st.button("Last ⏭️", key="colors_last"):
                    st.session_state.colors_page = total_pages
                    st.rerun()

            start_idx = (current_page - 1) * page_size
            end_idx = min(start_idx + page_size, len(filtered_colors))
            current_colors = filtered_colors[start_idx:end_idx]

            st.write(f"Showing pairs {start_idx + 1}-{end_idx} of {len(filtered_colors)}")

            # Compact card display
            cols_per_row = 4
            for i in range(0, len(current_colors), cols_per_row):
                cols = st.columns(cols_per_row)
                for j in range(cols_per_row):
                    idx = i + j
                    if idx < len(current_colors):
                        with cols[j]:
                            pair, color = current_colors[idx]
                            text_color = "white" if self._is_dark_color(color) else "black"
                            st.markdown(f"""
                                <div style="
                                    background-color: {color}; 
                                    color: {text_color};
                                    padding: 8px;
                                    border-radius: 4px;
                                    margin: 2px;
                                    font-size: 12px;
                                    text-align: center;
                                    border: 1px solid #ddd;
                                    word-break: break-all;
                                ">
                                <strong>{pair}</strong><br>
                                {color}
                                </div>
                                """, unsafe_allow_html=True)

            # Export button
            if st.button("📥 Export All Colors to CSV", key="export_colors"):
                df = pd.DataFrame(all_colors, columns=['Pair', 'Color'])
                csv = df.to_csv(index=False, encoding='utf-8')
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name="pair_colors.csv",
                    mime="text/csv"
                )

        finally:
            conn.close()

    def _is_dark_color(self, hex_color: str) -> bool:
        """Determine if color is dark"""
        if hex_color.startswith('#'):
            hex_color = hex_color[1:]

        if len(hex_color) == 3:
            hex_color = ''.join([c*2 for c in hex_color])

        try:
            r = int(hex_color[0:2], 16)
            g = int(hex_color[2:4], 16)
            b = int(hex_color[4:6], 16)

            # Luminance perception formula
            brightness = (0.299 * r + 0.587 * g + 0.114 * b) / 255
            return brightness < 0.5
        except:
            return False

    def _display_tables_compact(self):
        """Compact display of DB tables"""
        tables = self.storage.get_all_tables()

        if not tables:
            st.info("No tables found")
            return

        st.write(f"Total tables: {len(tables)}")

        # Search by table name
        search_term = st.text_input("🔍 Search by table name:", key="table_search")

        if search_term:
            tables = [t for t in tables if search_term.lower() in t.lower()]
            st.write(f"Found: {len(tables)} tables")

        # Pagination
        page_size = 50
        total_pages = (len(tables) + page_size - 1) // page_size

        if total_pages > 1:
            page = st.number_input(
                "Page:",
                min_value=1,
                max_value=total_pages,
                value=1,
                key="table_page"
            )
            start_idx = (page - 1) * page_size
            end_idx = min(start_idx + page_size, len(tables))
            current_tables = tables[start_idx:end_idx]
            st.write(f"Showing tables {start_idx+1}-{end_idx} of {len(tables)}")
        else:
            current_tables = tables

        # Use expandable widget
        with st.expander("📋 Show Tables", expanded=False):
            # Group tables by prefix
            table_groups = {}
            for table in current_tables:
                if '_' in table:
                    prefix = table.split('_')[0]
                else:
                    prefix = 'other'

                if prefix not in table_groups:
                    table_groups[prefix] = []
                table_groups[prefix].append(table)

            # Create accordion for each group
            for prefix, table_list in sorted(table_groups.items()):
                with st.expander(f"{prefix} ({len(table_list)} tables)", expanded=False):
                    # Split into columns for compactness
                    cols_per_row = 3
                    for i in range(0, len(table_list), cols_per_row):
                        cols = st.columns(cols_per_row)
                        for j in range(cols_per_row):
                            idx = i + j
                            if idx < len(table_list):
                                with cols[j]:
                                    st.text(table_list[idx])

        # Select specific table for detailed view
        st.markdown("---")
        selected_table = st.selectbox(
            "Select table for detailed view:",
            [""] + sorted(tables),
            key="table_select_detail"
        )

        if selected_table:
            self._display_table_content(selected_table)

    def _display_table_content(self, table_name: str):
        """Display table content"""
        try:
            conn = sqlite3.connect(self.storage.db_path)

            # Get table info
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()

            if not columns:
                st.warning(f"Table {table_name} has no columns")
                return

            # Show column information
            with st.expander("📊 Table Structure", expanded=True):
                col_info = []
                for col in columns:
                    col_info.append({
                        'ID': col[0],
                        'Name': col[1],
                        'Type': col[2],
                        'Nullable': 'No' if col[3] else 'Yes',
                        'Default Value': col[4],
                        'PK': 'Yes' if col[5] else 'No'
                    })

                df_columns = pd.DataFrame(col_info)
                st.dataframe(df_columns, use_container_width=True)

            # Show data with pagination
            with st.expander("📈 Table Data", expanded=True):
                # Row count
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                total_rows = cursor.fetchone()[0]
                st.write(f"Total rows: {total_rows}")

                # Pagination for data
                rows_per_page = 100
                total_pages = (total_rows + rows_per_page - 1) // rows_per_page

                if total_pages > 1:
                    data_page = st.number_input(
                        "Data page:",
                        min_value=1,
                        max_value=total_pages,
                        value=1,
                        key=f"data_page_{table_name}"
                    )
                    offset = (data_page - 1) * rows_per_page
                    query = f"SELECT * FROM {table_name} LIMIT {rows_per_page} OFFSET {offset}"
                    st.write(f"Showing rows {offset+1}-{min(offset+rows_per_page, total_rows)} of {total_rows}")
                else:
                    query = f"SELECT * FROM {table_name} LIMIT 5000"

                # Load data
                df = pd.read_sql_query(query, conn)

                if not df.empty:
                    st.dataframe(df, use_container_width=True, height=400)

                    # Export button
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label=f"📥 Download {len(df)} rows",
                        data=csv,
                        file_name=f"{table_name}.csv",
                        mime="text/csv"
                    )
                else:
                    st.info("Table is empty")

            conn.close()

        except Exception as e:
            st.error(f"Error reading table {table_name}: {str(e)}")