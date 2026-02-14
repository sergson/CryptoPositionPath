# data_collector.py (FIXED VERSION)
"""
Simplified data collector for prototype
"""
import asyncio
import threading
from typing import Optional
from async_fetcher import AsyncExchangeFetcher
from analytics_engine import AnalyticsEngine
from logger import perf_logger
import time
from datetime import datetime, timedelta, timezone


class DataCollector:
    """Data collector for prototype"""

    def __init__(self, storage):
        self.storage = storage
        self.analytics = AnalyticsEngine(storage)
        self.is_running = False
        self.thread: Optional[threading.Thread] = None
        self.quote_currency: Optional[str] = None
        self.pair_limit: int = 50
        self.retention_hours: int = 24
        self.logger = perf_logger.get_logger('data_collector', 'collector')
        self.last_collection_time = None  # Time of last successful data collection
        self.min_interval_seconds = 30  # Minimum interval between snapshot saves

    def start(self, exchange: str, market_type: str,
              quote_currency: Optional[str] = None,
              interval_seconds: int = 300,
              pair_limit: int = 50,
              retention_hours: int = 24):
        """Start data collection"""

        if self.is_running:
            self.logger.debug("⚠ Data collection already running")
            return

        self.is_running = True
        self.quote_currency = quote_currency
        self.pair_limit = min(pair_limit, 5000)  # Limit to max 1000 pairs
        self.retention_hours = retention_hours

        def collection_loop():
            """Data collection loop"""
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            while self.is_running:
                try:
                    # Check if enough time has passed since last collection
                    current_time = datetime.now(timezone.utc)
                    if (self.last_collection_time and
                            (current_time - self.last_collection_time).total_seconds() < self.min_interval_seconds):
                        self.logger.debug(f"⏸️ Skipping data collection, less than {self.min_interval_seconds} seconds passed")
                    else:
                        # Collect data
                        loop.run_until_complete(
                            self._collect_data(exchange, market_type)
                        )
                        self.last_collection_time = current_time

                    # Wait specified interval
                    for _ in range(interval_seconds):
                        if not self.is_running:
                            break
                        time.sleep(1)

                except Exception as e:
                    self.logger.error(f"❌ Error in collection loop: {e}")
                    time.sleep(30)

        # Start in separate thread
        self.thread = threading.Thread(
            target=collection_loop,
            daemon=True,
            name="DataCollectorThread"
        )
        self.thread.start()

        currency_info = f" (currency: {quote_currency})" if quote_currency else ""
        self.logger.info(f"✅ Data collection started: {exchange} {market_type}{currency_info} every {interval_seconds}s")

    def stop(self):
        """Safe stop of data collection"""
        self.is_running = False

        if self.thread:
            try:
                # Wait for thread to finish
                self.thread.join(timeout=10)
                if self.thread.is_alive():
                    self.logger.warning("⚠ Data collection thread did not finish in time")
            except Exception as e:
                self.logger.error(f"⚠ Error stopping thread: {e}")

            self.thread = None

        self.logger.info("⏹️ Data collection stopped")

    async def _collect_data(self, exchange: str, market_type: str):
        """Collect data once with manual colors update"""
        try:
            async with AsyncExchangeFetcher(exchange, market_type) as fetcher:
                df = await fetcher.fetch_ranked_pairs(
                    limit=self.pair_limit,
                    quote_currency=self.quote_currency
                )

                if not df.empty:
                    # Check if enough time has passed since last save
                    # Check latest snapshot in DB
                    latest_snapshots = self.storage.get_latest_snapshots(exchange, market_type, limit=1)

                    if latest_snapshots:
                        _, latest_time, _ = latest_snapshots[0]
                        current_time = datetime.now(timezone.utc)
                        time_diff = (current_time - latest_time).total_seconds()

                        if time_diff < self.min_interval_seconds:
                            self.logger.debug(f"⏸️ Skipping snapshot save, only {time_diff:.1f} seconds passed")
                            return

                    # Save snapshot
                    table_name = self.storage.save_snapshot(
                        exchange,
                        market_type,
                        df,
                        period_minutes=5
                    )

                    if table_name:
                        try:
                            # Call analysis
                            self.analytics.build_and_save_two_point_tracks(exchange, market_type, rebuild_all=False)
                        except Exception as e:
                            self.logger.error(f"❌ Error in analyze_trajectories: {e}")

                    self.logger.debug(f"📊 Data retrieved: {len(df)} pairs from {exchange}")

                    # Retention period control
                    self.storage.cleanup_old_data(
                        retention_hours=self.retention_hours,
                        cleanup_colors=False
                    )

        except Exception as e:
            self.logger.error(f"❌ Data collection error: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")