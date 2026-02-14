# analytics_engine.py (updated)
"""
Analytics module for determining price trajectories
"""
from typing import Dict, List, Optional
from data_storage import DataStorage
import pandas as pd
import sqlite3
from logger import perf_logger
import time
from datetime import datetime, timedelta, timezone


class AnalyticsEngine:
    """Analysis of position change trajectories for pairs"""

    def __init__(self, storage: DataStorage):
        self.storage = storage
        self.monotone_threshold = 0.7  # Monotonicity threshold
        self.min_consecutive = 3  # Minimum number of consecutive changes
        self.logger = perf_logger.get_logger('analytics_engine', 'analytics')

    def build_and_save_two_point_tracks(self, exchange: str, market_type: str,
                                        rebuild_all: bool = False,
                                        target_interval_seconds: int = None):
        """Building and saving tracks from snapshots

        Args:
            exchange: Exchange name
            market_type: Market type
            rebuild_all: If True - iterate over snapshots in DB considering interval
            target_interval_seconds: Target interval between snapshots (in seconds)
        """
        try:
            from track_builder import TrackBuilder

            # Create tracks table if needed
            self.storage.create_tracks_table()

            # Create track builder
            track_builder = TrackBuilder(self.storage)

            # Get threshold from settings
            rank_threshold = self.storage.get_setting('rank_threshold', 5)

            # If target_interval_seconds not provided, get from settings
            if target_interval_seconds is None:
                target_interval_seconds = self.storage.get_setting('interval', 60)
                # Convert string to int if needed
                if isinstance(target_interval_seconds, str):
                    try:
                        target_interval_seconds = int(target_interval_seconds)
                    except:
                        target_interval_seconds = 60
                self.logger.debug(f"📊 Using interval from settings: {target_interval_seconds} sec")

            # Delete existing tracks for this exchange and market type
            if rebuild_all:
                self._delete_tracks_for_exchange(exchange, market_type)
                self.logger.info(f"🗑️ Deleted existing tracks for {exchange}/{market_type}")

            # Get snapshots based on mode
            if rebuild_all:
                # Get all snapshots for this exchange and market type
                snapshots = self._get_all_snapshots_sorted(exchange, market_type)
                self.logger.info(f"📊 Rebuild mode: found {len(snapshots)} snapshots")

                if len(snapshots) > 0:
                    # Analyze actual interval between snapshots
                    self._analyze_snapshot_intervals(snapshots)
            else:
                # Get the last two snapshots
                snapshots = self.storage.get_latest_snapshots(exchange, market_type, limit=2)
                self.logger.debug(f"📊 Normal mode: last 2 snapshots")

            if len(snapshots) < 2:
                self.logger.warning("Not enough snapshots to build tracks")
                return

            all_tracks = {}
            created_count = 0
            processed_pairs = set()

            if rebuild_all:
                # Process snapshots with target interval
                self.logger.info(f"🔄 Building tracks with target interval: {target_interval_seconds} sec")

                # Algorithm to skip snapshots to approximate target interval
                i = 0
                skipped_count = 0
                used_count = 0

                while i < len(snapshots):
                    # Find the next snapshot closest to target interval
                    current_table, current_time, _ = snapshots[i]
                    best_match_index = -1
                    best_match_diff = float('inf')

                    # Look ahead for snapshots close to target interval
                    for j in range(i + 1, min(i + 20, len(snapshots))):  # Check at most 20 next snapshots
                        next_table, next_time, _ = snapshots[j]
                        time_diff = (next_time - current_time).total_seconds()
                        diff_from_target = abs(time_diff - target_interval_seconds)

                        # Allow 30 seconds tolerance
                        if diff_from_target <= 30:
                            best_match_index = j
                            best_match_diff = diff_from_target
                            break
                        elif diff_from_target < best_match_diff:
                            best_match_index = j
                            best_match_diff = diff_from_target

                    if best_match_index != -1:
                        # Found a suitable snapshot
                        prev_table, prev_time, _ = snapshots[i]
                        new_table, new_time, _ = snapshots[best_match_index]
                        actual_interval = (new_time - prev_time).total_seconds()

                        if abs(actual_interval - target_interval_seconds) <= 30:
                            self.logger.debug(
                                f"✅ Perfect interval: {actual_interval:.0f} sec between {prev_table} and {new_table}")
                        else:
                            self.logger.info(
                                f"⚠ Approximate interval: {actual_interval:.0f} sec (target: {target_interval_seconds} sec)")

                        created_in_pair = self._process_snapshot_pair(
                            track_builder, prev_table, prev_time, new_table, new_time,
                            exchange, market_type, rank_threshold, all_tracks, processed_pairs
                        )
                        created_count += created_in_pair
                        used_count += 1

                        # Move to the snapshot after new_table
                        i = best_match_index
                    else:
                        # No suitable snapshot, take the next one in order
                        if i + 1 < len(snapshots):
                            prev_table, prev_time, _ = snapshots[i]
                            new_table, new_time, _ = snapshots[i + 1]
                            actual_interval = (new_time - prev_time).total_seconds()

                            if actual_interval > target_interval_seconds * 2:
                                self.logger.warning(
                                    f"⚠ Large interval: {actual_interval:.0f} sec ({actual_interval / target_interval_seconds:.1f} times target)")

                            created_in_pair = self._process_snapshot_pair(
                                track_builder, prev_table, prev_time, new_table, new_time,
                                exchange, market_type, rank_threshold, all_tracks, processed_pairs
                            )
                            created_count += created_in_pair
                            used_count += 1
                            i += 1
                        else:
                            break

                    # Count skipped snapshots
                    skipped_count = len(snapshots) - used_count

                    # Log progress
                    if used_count % 10 == 0:
                        self.logger.info(f"⏳ Processed {used_count} snapshot pairs, skipped {skipped_count}")

                self.logger.info(
                    f"📊 Total: used {used_count} snapshot pairs out of {len(snapshots)}, skipped {skipped_count}")
            else:
                # Process only last 2 snapshots
                new_table, new_time, _ = snapshots[0]
                prev_table, prev_time, _ = snapshots[1]

                created_in_pair = self._process_snapshot_pair(
                    track_builder, prev_table, prev_time, new_table, new_time,
                    exchange, market_type, rank_threshold, all_tracks, processed_pairs
                )
                created_count += created_in_pair

            # Save tracks to DB
            if all_tracks:
                track_builder.save_tracks_to_db(all_tracks, exchange, market_type)
                mode_text = "rebuilt" if rebuild_all else "created"
                interval_text = f"with interval ~{target_interval_seconds} sec" if rebuild_all else ""
                self.logger.debug(f"✅ {mode_text} {created_count} tracks for {len(all_tracks)} pairs {interval_text}")
            else:
                self.logger.info("⚠️ No tracks to save")

        except Exception as e:
            self.logger.error(f"❌ Error building tracks: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")

    def _process_snapshot_pair(self, track_builder, prev_table, prev_time, new_table, new_time,
                               exchange, market_type, rank_threshold, all_tracks, processed_pairs):
        """Process a snapshot pair"""
        created_in_pair = 0

        # Load snapshot data
        new_df = self.storage.get_snapshot_data(new_table)
        prev_df = self.storage.get_snapshot_data(prev_table)

        if new_df.empty or prev_df.empty:
            return 0

        # Get manual pairs
        manual_pairs = self.storage.get_manual_pairs()

        # Find common pairs in both snapshots
        common_pairs = set(new_df['pair']).intersection(set(prev_df['pair']))

        for pair in common_pairs:
            # Skip if already processed (for rebuild_all)
            pair_key = f"{pair}_{prev_time}_{new_time}"
            if pair_key in processed_pairs:
                continue

            try:
                # Get ranks
                new_rank = new_df[new_df['pair'] == pair]['rank'].iloc[0]
                prev_rank = prev_df[prev_df['pair'] == pair]['rank'].iloc[0]

                # Calculate difference
                rank_diff = abs(int(new_rank) - int(prev_rank))

                # Check conditions
                is_manual = pair in manual_pairs
                if is_manual or rank_diff >= rank_threshold:
                    # Get color
                    color_id, color_hex = self.storage.get_or_create_pair_color(pair)

                    # Create track
                    track = track_builder._create_track_from_two_points(
                        pair, prev_df, prev_time, new_df, new_time,
                        color_hex if color_hex else "#FF0000",
                        is_manual
                    )

                    if track:
                        if pair not in all_tracks:
                            all_tracks[pair] = []
                        all_tracks[pair].append(track)
                        created_in_pair += 1
                        processed_pairs.add(pair_key)

            except Exception as e:
                self.logger.warning(f"Error creating track for {pair}: {e}")

        return created_in_pair

    def _get_all_snapshots_sorted(self, exchange: str, market_type: str):
        """Get all snapshots for exchange and market type, sorted by time"""
        conn = sqlite3.connect(self.storage.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT table_name, exchange_timestamp, created_at 
                FROM snapshots_meta 
                WHERE exchange = ? AND market_type = ?
                ORDER BY exchange_timestamp ASC
            ''', (exchange, market_type))

            results = cursor.fetchall()

            if not results:
                return []

            # Convert time strings to datetime
            snapshots = []
            for table_name, exchange_time_str, created_str in results:
                try:
                    exchange_time = datetime.fromisoformat(exchange_time_str.replace('Z', '+00:00'))
                except:
                    # If cannot parse as ISO, use created_at as fallback
                    exchange_time = datetime.fromisoformat(created_str)

                created_at = datetime.fromisoformat(created_str)
                snapshots.append((table_name, exchange_time, created_at))

            return snapshots

        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                self.logger.warning("⚠ Table snapshots_meta does not exist")
                return []
            raise
        finally:
            conn.close()

    def _analyze_snapshot_intervals(self, snapshots):
        """Analyze intervals between snapshots"""
        if len(snapshots) < 2:
            return

        intervals = []
        for i in range(len(snapshots) - 1):
            _, time1, _ = snapshots[i]
            _, time2, _ = snapshots[i + 1]
            interval = (time2 - time1).total_seconds()
            intervals.append(interval)

        if intervals:
            min_interval = min(intervals)
            max_interval = max(intervals)
            avg_interval = sum(intervals) / len(intervals)

            self.logger.info(
                f"📊 Interval statistics: min={min_interval:.0f}s, max={max_interval:.0f}s, avg={avg_interval:.0f}s")

            # Group intervals
            from collections import Counter
            interval_counts = Counter(intervals)
            most_common = interval_counts.most_common(3)

            self.logger.info(f"📊 Most frequent intervals:")
            for interval, count in most_common:
                self.logger.info(f"  - {interval:.0f}s: {count} times ({count / len(intervals) * 100:.1f}%)")

    def _delete_tracks_for_exchange(self, exchange: str, market_type: str):
        """Delete all tracks for specified exchange and market type"""
        conn = sqlite3.connect(self.storage.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute('''
                DELETE FROM tracks 
                WHERE exchange = ? AND market_type = ?
            ''', (exchange, market_type))

            deleted_count = cursor.rowcount
            conn.commit()

            self.logger.info(f"🗑️ Deleted {deleted_count} tracks for {exchange}/{market_type}")

        except Exception as e:
            self.logger.error(f"❌ Error deleting tracks: {e}")
            conn.rollback()
        finally:
            conn.close()

    def rebuild_all_tracks(self, exchange: str, markets: List[str], interval_seconds: int = None):
        """Rebuild all tracks for specified markets considering interval

        Args:
            exchange: Exchange name
            markets: List of market types
            interval_seconds: Target interval between snapshots (if None, taken from settings)
        """
        self.logger.info(f"🔄 Started rebuilding all tracks for {exchange}")

        total_created = 0

        for market_type in markets:
            self.logger.info(f"🔄 Rebuilding tracks for market: {market_type}")

            try:
                # Get current threshold for logging
                rank_threshold = self.storage.get_setting('rank_threshold', 5)
                self.logger.info(f"📊 Using sensitivity threshold: {rank_threshold}")

                # Get interval from settings if not provided
                if interval_seconds is None:
                    interval_setting = self.storage.get_setting('interval', 60)
                    if isinstance(interval_setting, str):
                        try:
                            interval_seconds = int(interval_setting)
                        except:
                            interval_seconds = 60
                    else:
                        interval_seconds = interval_setting

                self.logger.info(f"📊 Target interval between snapshots: {interval_seconds} sec")

                # Build tracks with rebuild_all=True and specified interval
                self.build_and_save_two_point_tracks(
                    exchange, market_type,
                    rebuild_all=True,
                    target_interval_seconds=interval_seconds
                )

            except Exception as e:
                self.logger.error(f"❌ Error rebuilding tracks for {market_type}: {e}")
                import traceback
                self.logger.error(f"Traceback: {traceback.format_exc()}")

        self.logger.info(f"✅ Completed rebuilding all tracks for {exchange}")