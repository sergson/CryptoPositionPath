# manual_tracks_manager.py (supplemented new version)
"""
Manager for handling manual tracks
"""
from typing import Dict, List, Optional, Tuple
from track_builder import TrackBuilder, TrackSegment, TrackPoint
from data_storage import DataStorage
from logger import perf_logger
import sqlite3
from datetime import datetime, timedelta
import json


class ManualTracksManager:
    """Manual tracks manager"""

    def __init__(self, storage: DataStorage):
        self.storage = storage
        self.track_builder = TrackBuilder(storage)
        self.logger = perf_logger.get_logger('manual_tracks_manager', 'analytics')

    def remove_manual_tracks(self, pair: str, exchange: str, market_type: str):
        """Remove tracks of a manual pair (ported from old version)"""
        conn = sqlite3.connect(self.storage.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute('''
                DELETE FROM tracks 
                WHERE pair = ? 
                AND exchange = ? 
                AND market_type = ?
                AND json_extract(track_data, '$[0].track_type') = 'manual'
            ''', (pair, exchange, market_type))

            conn.commit()
            self.logger.info(f"✅ Removed manual tracks for pair: {pair}")
        except Exception as e:
            self.logger.error(f"❌ Error removing tracks for {pair}: {e}")
            conn.rollback()
        finally:
            conn.close()