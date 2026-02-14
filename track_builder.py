# track_builder.py (fixed version)
"""
Module for building and approximating trajectory tracks
"""
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional
import json
from dataclasses import dataclass
from scipy import stats
from logger import perf_logger
import time
import pandas as pd


@dataclass
class TrackPoint:
    """Track point"""
    time: datetime
    rank: int
    price: float
    change: float
    volume: float
    color: Optional[str] = None
    is_manual: bool = False
    is_highlighted: bool = False


@dataclass
class TrackSegment:
    """Track segment"""
    pair: str
    points: List[TrackPoint]
    direction: str  # 'up', 'down', 'flat'
    start_time: datetime
    end_time: datetime
    start_rank: int
    end_rank: int
    control_point: Tuple[float, float]
    color: str
    track_type: str  # 'auto', 'manual'
    is_manual: bool = False
    error_score: float = 0.0
    last_highlighted_time: Optional[datetime] = None  # Time of the last highlighted point in the track


class TrackBuilder:
    """Trajectory track builder with optimization"""

    def __init__(self, storage):
        self.storage = storage
        self.logger = perf_logger.get_logger('track_builder', 'analytics')

    def save_tracks_to_db(self, tracks: Dict[str, List[TrackSegment]],
                          exchange: str, market_type: str):
        """
        Save tracks to the database (without uniqueness check)
        datetime.now(tz=timezone.utc).isoformat()
        """
        conn = sqlite3.connect(self.storage.db_path)
        cursor = conn.cursor()

        try:
            # Current UTC time for created_at and updated_at
            current_utc_time = datetime.now(tz=timezone.utc).isoformat()
            # Save tracks
            saved_count = 0
            for pair, track_list in tracks.items():
                if not track_list:
                    continue

                for track in track_list:
                    # Convert track to JSON
                    track_dict = {
                        'points': [
                            {
                                'time': p.time.isoformat(),
                                'rank': p.rank,
                                'price': p.price,
                                'change': p.change,
                                'volume': p.volume,
                                'color': p.color,
                                'is_manual': p.is_manual,
                                'is_highlighted': p.is_highlighted
                            } for p in track.points
                        ],
                        'direction': track.direction,
                        'start_time': track.start_time.isoformat(),
                        'end_time': track.end_time.isoformat(),
                        'start_rank': track.start_rank,
                        'end_rank': track.end_rank,
                        'control_point': track.control_point,
                        'color': track.color,
                        'track_type': track.track_type,
                        'error_score': track.error_score
                    }

                    track_data_json = json.dumps(track_dict)

                    # Insert track WITHOUT uniqueness check
                    cursor.execute('''
                        INSERT INTO tracks 
                         (pair, exchange, market_type, track_data, last_highlighted_time, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (pair, exchange, market_type, track_data_json,
                          track.last_highlighted_time.isoformat() if track.last_highlighted_time else None,
                          current_utc_time, current_utc_time))

                    saved_count += 1

            conn.commit()
            self.logger.debug(f"✅ Saved {saved_count} tracks to DB for {exchange}/{market_type}")

        except Exception as e:
            self.logger.error(f"❌ Error saving tracks: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    def load_tracks_from_db(self, exchange: str, market_type: str,
                            pair: str = None,
                            lookback_hours: int = 24) -> Dict[str, List[TrackSegment]]:
        """
        Load tracks from the database with time filtering
        """
        conn = sqlite3.connect(self.storage.db_path)
        cursor = conn.cursor()

        try:
            # Calculate cutoff time
            from datetime import timezone
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)

            if pair:
                cursor.execute('''
                    SELECT pair, track_data 
                    FROM tracks 
                    WHERE exchange = ? AND market_type = ? 
                    AND pair = ? 
                    AND created_at >= ?
                    ORDER BY created_at DESC
                ''', (exchange, market_type, pair,
                      cutoff_time.isoformat()))
            else:
                cursor.execute('''
                    SELECT pair, track_data 
                    FROM tracks 
                    WHERE exchange = ? AND market_type = ? 
                    AND created_at >= ?
                    ORDER BY created_at DESC
                ''', (exchange, market_type,
                      cutoff_time.isoformat()))

            all_tracks = {}

            for row in cursor.fetchall():
                pair_name, track_data_json = row

                try:
                    track_data = json.loads(track_data_json)

                    # FIX: Check if track_data is a list or a dictionary
                    if isinstance(track_data, dict):
                        track_list = [track_data]
                    elif isinstance(track_data, list):
                        track_list = track_data
                    else:
                        self.logger.warning(f"Invalid track format for {pair_name}: {type(track_data)}")
                        continue

                    tracks_for_pair = []  # <-- ADDED this variable

                    for track_dict in track_list:
                        # Check that track_dict is actually a dictionary
                        if not isinstance(track_dict, dict):
                            self.logger.warning(f"Invalid track element for {pair_name}: {type(track_dict)}")
                            continue

                        # Check for required fields
                        if 'points' not in track_dict:
                            self.logger.warning(f"Missing points in track for {pair_name}")
                            continue

                        # Reconstruct track point objects
                        points = []
                        for p in track_dict['points']:
                            # Check point structure
                            if not isinstance(p, dict):
                                continue

                            try:
                                dt = datetime.fromisoformat(p['time'])
                                if dt.tzinfo is None:
                                    dt = dt.replace(tzinfo=timezone.utc)

                                points.append(TrackPoint(
                                    time=dt,
                                    rank=p['rank'],
                                    price=p['price'],
                                    change=p['change'],
                                    volume=p['volume'],
                                    color=p.get('color'),
                                    is_manual=p.get('is_manual', False),
                                    is_highlighted=p.get('is_highlighted', False)
                                ))
                            except (KeyError, ValueError) as e:
                                self.logger.warning(f"Error processing track point for {pair_name}: {e}")
                                continue

                        if not points:
                            continue

                        try:
                            start_dt = datetime.fromisoformat(track_dict['start_time'])
                            if start_dt.tzinfo is None:
                                start_dt = start_dt.replace(tzinfo=timezone.utc)

                            end_dt = datetime.fromisoformat(track_dict['end_time'])
                            if end_dt.tzinfo is None:
                                end_dt = end_dt.replace(tzinfo=timezone.utc)
                        except (KeyError, ValueError) as e:
                            self.logger.warning(f"Error with track time for {pair_name}: {e}")
                            continue

                        # Reconstruct last_highlighted_time
                        last_highlighted_time = None
                        for point in reversed(points):
                            if point.is_highlighted:
                                last_highlighted_time = point.time
                                break

                        try:
                            track = TrackSegment(
                                pair=pair_name,
                                points=points,
                                direction=track_dict.get('direction', 'flat'),
                                start_time=start_dt,
                                end_time=end_dt,
                                start_rank=track_dict.get('start_rank', 0),
                                end_rank=track_dict.get('end_rank', 0),
                                control_point=tuple(track_dict.get('control_point', (0, 0))),
                                color=track_dict.get('color', '#FFFFFF'),
                                track_type=track_dict.get('track_type', 'auto'),
                                error_score=track_dict.get('error_score', 0.0),
                                last_highlighted_time=last_highlighted_time
                            )
                            tracks_for_pair.append(track)  # <-- FIXED: was tracks.append(track)
                        except Exception as e:
                            self.logger.error(f"Error creating TrackSegment for {pair_name}: {e}")
                            continue

                    # FIX: Add all tracks for the pair, not overwrite
                    if tracks_for_pair:
                        if pair_name not in all_tracks:
                            all_tracks[pair_name] = []
                        all_tracks[pair_name].extend(tracks_for_pair)  # <-- Use extend instead of assignment

                except json.JSONDecodeError as e:
                    self.logger.error(f"JSON decode error for {pair_name}: {e}")
                except Exception as e:
                    self.logger.error(f"Error loading track for {pair_name}: {e}")

            # Add logging for debugging
            total_tracks = sum(len(tracks) for tracks in all_tracks.values())
            if total_tracks > 0:
                self.logger.debug(f"Loaded {total_tracks} tracks from DB for {exchange}/{market_type}")
                for pair_name, track_list in all_tracks.items():
                    self.logger.debug(f"  Pair {pair_name}: {len(track_list)} tracks")

            return all_tracks

        except Exception as e:
            self.logger.error(f"Error loading tracks from DB: {e}")
            return {}
        finally:
            conn.close()

    def _create_track_from_two_points(self, pair: str,
                                      prev_df: pd.DataFrame, prev_time: datetime,
                                      new_df: pd.DataFrame, new_time: datetime,
                                      color_hex: str, is_manual: bool) -> TrackSegment:
        """Create a track from two points (two snapshots)"""
        from datetime import timezone

        try:
            # Get data for the first point (previous snapshot)
            prev_row = prev_df[prev_df['pair'] == pair].iloc[0]
            prev_point = TrackPoint(
                time=prev_time,
                rank=int(prev_row['rank']),
                price=float(prev_row['price']),
                change=float(prev_row['change_24h']),
                volume=float(prev_row['volume_24h']),
                color=color_hex,
                is_manual=is_manual,
                is_highlighted=True  # Points in the track are always highlighted
            )

            # Get data for the second point (new snapshot)
            new_row = new_df[new_df['pair'] == pair].iloc[0]
            new_point = TrackPoint(
                time=new_time,
                rank=int(new_row['rank']),
                price=float(new_row['price']),
                change=float(new_row['change_24h']),
                volume=float(new_row['volume_24h']),
                color=color_hex,
                is_manual=is_manual,
                is_highlighted=True
            )

            points = [prev_point, new_point]

            # Determine direction
            if new_point.rank < prev_point.rank:
                direction = 'up'  # Rank decreased (improved position)
            elif new_point.rank > prev_point.rank:
                direction = 'down'  # Rank increased (worsened position)
            else:
                direction = 'flat'  # Ranks equal

            # Time of the last highlighted point
            last_highlighted_time = new_time

            # Control point for visualization (middle of the segment)
            control_x = (new_time - prev_time).total_seconds() / 60 / 2  # Midpoint in minutes
            control_y = (prev_point.rank + new_point.rank) / 2  # Midpoint in rank

            track_type = 'manual' if is_manual else 'auto'

            track = TrackSegment(
                pair=pair,
                points=points,
                direction=direction,
                start_time=prev_time,
                end_time=new_time,
                start_rank=prev_point.rank,
                end_rank=new_point.rank,
                control_point=(control_x, control_y),
                color=color_hex,
                track_type=track_type,
                is_manual=is_manual,
                error_score=0.0,  # For a simple track, error is always 0
                last_highlighted_time=last_highlighted_time)
            return track

        except Exception as e:
            self.logger.error(f"Error creating track for {pair}: {e}")
            return None