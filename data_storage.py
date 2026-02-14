# data_storage.py
"""Data storage module for SQLite database"""
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple, Any
import json
import random
from logger import perf_logger
import time


class DataStorage:
    """Managing data storage in SQLite"""

    def __init__(self, db_path: str = "crypto_data.db"):
        self.db_path = db_path
        self.logger = perf_logger.get_logger('data_storage', 'db')
        # Initialize caches
        self._manual_pairs_cache = None
        self._manual_pairs_cache_time = 0
        self._manual_pairs_cache_ttl = 60  # 60 seconds
        self._pair_color_cache = {}
        self._pair_color_cache_time = {}
        self._pair_color_cache_ttl = 300  # 300 seconds = 5 minutes
        self.logger.debug(f"✅ Initializing DataStorage: {db_path}")
        start_time = time.time()
        self._init_database()
        self._verify_integrity()
        elapsed = time.time() - start_time
        if elapsed > 1.0:
            self.logger.warning(f"Database initialization took {elapsed:.3f} sec")
        else:
            self.logger.debug(f"Database initialization took {elapsed:.3f} sec")

    def _init_database(self):
        """Initialize database structure"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Table for storing pair colors (unique color per pair)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pair_colors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair TEXT UNIQUE NOT NULL,
                color TEXT NOT NULL UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_system INTEGER DEFAULT 0  -- 1 for black/white
            )
        ''')

        # Table for storing all used pairs (for manual selection)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS used_pairs (
                pair TEXT PRIMARY KEY,
                exchange TEXT NOT NULL,
                market_type TEXT NOT NULL,
                quote_currency TEXT,
                first_seen TIMESTAMP,
                last_seen TIMESTAMP
            )
        ''')

        # Table for snapshot metadata
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS snapshots_meta (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT UNIQUE NOT NULL,
                exchange TEXT NOT NULL,
                market_type TEXT NOT NULL,
                exchange_timestamp TIMESTAMP NOT NULL,
                created_at TIMESTAMP NOT NULL,
                period_minutes INTEGER NOT NULL,
                row_count INTEGER NOT NULL,
                FOREIGN KEY (table_name) REFERENCES sqlite_master(name) ON DELETE CASCADE
            )
        ''')

        # Table for storing user settings
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_settings (
                setting_key TEXT PRIMARY KEY,
                setting_value TEXT,
                last_updated TIMESTAMP
            )
        ''')

        # Indexes
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_snapshots_exchange_time 
            ON snapshots_meta(exchange_timestamp)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_snapshots_created_at 
            ON snapshots_meta(created_at)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_snapshots_exchange_market 
            ON snapshots_meta(exchange, market_type)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_used_pairs_exchange 
            ON used_pairs(exchange, market_type, quote_currency)
        ''')

        # Add system colors (black and white)
        cursor.execute('''
            INSERT OR IGNORE INTO pair_colors (pair, color, is_system) 
            VALUES (?, ?, ?)
        ''', ('BLACK', '#000000', 1))
        cursor.execute('''
            INSERT OR IGNORE INTO pair_colors (pair, color, is_system) 
            VALUES (?, ?, ?)
        ''', ('WHITE', '#FFFFFF', 1))

        # Save default settings
        default_settings = {
            'exchange': 'binance',
            'quote_currency': 'USDT',
            'markets': json.dumps(['spot']),
            'interval': '60',
            'retention': '24',
            'pair_limit': '800',
            'manual_pairs': json.dumps([])
        }

        for key, value in default_settings.items():
            cursor.execute('''
                INSERT OR IGNORE INTO user_settings (setting_key, setting_value, last_updated)
                VALUES (?, ?, ?)
            ''', (key, value, datetime.now().isoformat()))

        conn.commit()
        conn.close()

        # CREATE TRACKS TABLE AFTER MAIN INITIALIZATION
        self.create_tracks_table()

    def verify_db_integrity(self):
        self._verify_integrity()

    def _verify_integrity(self):
        """Check database integrity on startup"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # 1. Check snapshots_meta against actual tables
            cursor.execute('SELECT table_name FROM snapshots_meta')
            meta_tables = [row[0] for row in cursor.fetchall()]

            for table_name in meta_tables:
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table_name,)
                )
                if not cursor.fetchone():
                    self.logger.info(f"⚠ Deleting entry from snapshots_meta for non-existent table: {table_name}")
                    cursor.execute('DELETE FROM snapshots_meta WHERE table_name=?', (table_name,))

            # 2. Check color uniqueness in pair_colors
            cursor.execute('''
                SELECT color, COUNT(*) as cnt 
                FROM pair_colors 
                WHERE is_system = 0 
                GROUP BY color 
                HAVING cnt > 1
            ''')
            duplicate_colors = cursor.fetchall()
            for color, count in duplicate_colors:
                self.logger.info(f"⚠ Found duplicate color {color}: {count} entries")

            # 3. Check and fix colors in snapshots
            self.verify_and_fix_snapshot_colors()

            conn.commit()
        except Exception as e:
            self.logger.info(f"⚠ Integrity check error: {e}")
            conn.rollback()
        finally:
            conn.close()

    def save_snapshot(self, exchange: str, market_type: str,
                      df: pd.DataFrame, period_minutes: int = 5) -> str:
        """
        Save data snapshot with system time
        """
        start_time = time.time()
        if df.empty:
            return ""

        # Create table name with created_at
        # Use the first created_at from data as a reference
        created_at_timestamp = None
        if 'created_at' in df.columns and len(df) > 0:
            try:
                # Take the recording time from the first record
                first_created_at = df.iloc[0]['created_at']
                if isinstance(first_created_at, str):
                    # Attempt to parse ISO timestamp
                    try:
                        dt = datetime.fromisoformat(first_created_at.replace('Z', '+00:00'))
                        # Format for table name
                        created_at_timestamp = dt.strftime("%Y%m%d_%H%M%S")
                    except:
                        pass
            except Exception as e:
                self.logger.warning(f"Could not extract snapshot time for table name: {e}")

        # If exchange time could not be obtained, use system time
        if not created_at_timestamp:
            created_at_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        table_name = f"snapshot_{exchange}_{market_type}_{created_at_timestamp}"

        conn = sqlite3.connect(self.db_path)

        try:
            # Add color columns if missing
            if 'colour' not in df.columns:
                df['colour'] = None  # Reference to pair_colors.id
            if 'manual_colour' not in df.columns:
                df['manual_colour'] = None  # Manual highlighting

            # Save DataFrame to new table
            df.to_sql(table_name, conn, if_exists='replace', index=False)

            # Determine exchange time for the snapshot (average time from data)
            snapshot_exchange_time = None
            if 'timestamp' in df.columns and len(df) > 0:
                try:
                    # Take the first exchange time as snapshot time
                    first_time_str = df.iloc[0]['timestamp']
                    if isinstance(first_time_str, str):
                        snapshot_exchange_time = first_time_str
                except:
                    pass

            # If exchange time could not be obtained, use system time
            if not snapshot_exchange_time:
                snapshot_exchange_time = datetime.now().isoformat()

            # Add metadata with exchange time
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO snapshots_meta 
                (table_name, exchange, market_type, exchange_timestamp, created_at, period_minutes, row_count)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (table_name, exchange, market_type,
                  snapshot_exchange_time,  # Exchange time
                  datetime.now(tz=timezone.utc).isoformat(),  # System save time
                  period_minutes, len(df)))

            # Update used_pairs
            for pair in df['pair'].unique():
                cursor.execute('''
                    INSERT OR REPLACE INTO used_pairs 
                    (pair, exchange, market_type, first_seen, last_seen)
                    VALUES (?, ?, ?, COALESCE((SELECT first_seen FROM used_pairs WHERE pair=?), ?), ?)
                ''', (pair, exchange, market_type, pair, datetime.now().isoformat(),
                      datetime.now().isoformat()))

            conn.commit()
            self.logger.info(
                f"💾 Snapshot saved: {table_name} ({len(df)} records, exchange time: {snapshot_exchange_time[:19]})")

        except Exception as e:
            self.logger.error(f"❌ Error saving snapshot: {e}")
            table_name = ""
        finally:
            conn.close()

        elapsed = time.time() - start_time
        if elapsed > 0.5:
            self.logger.warning(f"save_snapshot took {elapsed:.3f} sec: {table_name}")
        else:
            self.logger.debug(f"save_snapshot took {elapsed:.3f} sec: {table_name}")
        return table_name

    def _generate_unique_color(self) -> str:
        """Generate unique color (8192 variants)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # 2^13 = 8192 colors (13 bits)
            # RGB: 5-5-3 bits (32×32×8 = 8192 combinations)
            used_colors = set()
            cursor.execute('SELECT color FROM pair_colors WHERE is_system = 0')
            for row in cursor.fetchall():
                if row and row[0]:
                    used_colors.add(row[0])

            max_attempts = 100
            for _ in range(max_attempts):
                # Generate 13-bit color (5-5-3)
                r = random.randint(0, 31)  # 5 bits (0-31)
                g = random.randint(0, 31)  # 5 bits (0-31)
                b = random.randint(0, 7)   # 3 bits (0-7)

                # Scale to 8 bits per channel
                r_8bit = int(r * 255 / 31)
                g_8bit = int(g * 255 / 31)
                b_8bit = int(b * 255 / 7)

                color_hex = f"#{r_8bit:02x}{g_8bit:02x}{b_8bit:02x}"

                # Check uniqueness and not too light/dark
                if color_hex not in used_colors and color_hex not in ['#000000', '#FFFFFF']:
                    luminance = 0.299 * (r_8bit / 255) + 0.587 * (g_8bit / 255) + 0.114 * (b_8bit / 255)
                    if 0.2 < luminance < 0.9:
                        return color_hex

            # Fallback
            return f"#{random.randint(0, 0xFFFFFF):06x}"
        except Exception as e:
            self.logger.error(f"Color generation error: {e}")
            # Return random color on error
            return f"#{random.randint(0, 0xFFFFFF):06x}"
        finally:
            conn.close()

    def update_snapshot_color(self, table_name: str, pair: str, color_id: int):
        """Update pair color in a specific snapshot"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute(f'''
                UPDATE {table_name} 
                SET colour = ? 
                WHERE pair = ?
            ''', (color_id, pair))
            conn.commit()
        except Exception as e:
            self.logger.error(f"❌ Error updating color in snapshot: {e}")
            conn.rollback()
        finally:
            conn.close()

    def get_used_pairs(self, exchange: str, market_type: str,
                       quote_currency: str = None) -> List[str]:
        """Get list of used pairs for manual selection"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            if quote_currency and quote_currency != "All pairs":
                cursor.execute('''
                    SELECT DISTINCT pair FROM used_pairs 
                    WHERE exchange = ? AND market_type = ? 
                    AND (quote_currency = ? OR quote_currency IS NULL)
                    ORDER BY pair
                ''', (exchange, market_type, quote_currency))
            else:
                cursor.execute('''
                    SELECT DISTINCT pair FROM used_pairs 
                    WHERE exchange = ? AND market_type = ?
                    ORDER BY pair
                ''', (exchange, market_type))

            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            self.logger.warning(f"⚠ Error getting pair list: {e}")
            return []
        finally:
            conn.close()

    def save_manual_pairs(self, pairs: List[str]):
        """Save selected pairs for manual highlighting"""
        self.save_setting('manual_pairs', json.dumps(pairs))
        self.invalidate_manual_pairs_cache()

    def get_manual_pairs(self) -> List[str]: #cached version
        """Get list of pairs for manual highlighting with TTL caching"""
        current_time = time.time()

        # Check cache (TTL = 30 seconds)
        if (hasattr(self, '_manual_pairs_cache') and
                hasattr(self, '_manual_pairs_cache_time') and
                self._manual_pairs_cache is not None and
                current_time - self._manual_pairs_cache_time < self._manual_pairs_cache_ttl):
            return self._manual_pairs_cache

        # Read from DB
        manual_pairs = self.get_setting('manual_pairs', '[]')

        if isinstance(manual_pairs, str):
            try:
                result = json.loads(manual_pairs)
                self._manual_pairs_cache = result
                self._manual_pairs_cache_time = current_time
                return result
            except json.JSONDecodeError:
                self._manual_pairs_cache = []
                self._manual_pairs_cache_time = current_time
                return []
        elif isinstance(manual_pairs, list):
            self._manual_pairs_cache = manual_pairs
            self._manual_pairs_cache_time = current_time
            return manual_pairs
        else:
            self._manual_pairs_cache = []
            self._manual_pairs_cache_time = current_time
            return []

    def invalidate_manual_pairs_cache(self):
        """Invalidate manual pairs cache"""
        self._manual_pairs_cache = None
        self._manual_pairs_cache_time = 0

    def get_latest_snapshots(self, exchange: str, market_type: str,
                             limit: int = 1440) -> List[Tuple[str, datetime, datetime]]:
        """
        Get latest snapshots with exchange time

        Returns:
            List[Tuple[table_name, exchange_timestamp, created_at]]
        """
        start_time = time.time()
        conn = sqlite3.connect(self.db_path)

        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT table_name, exchange_timestamp, created_at 
                FROM snapshots_meta 
                WHERE exchange = ? AND market_type = ?
                ORDER BY exchange_timestamp DESC 
                LIMIT ?
            ''', (exchange, market_type, limit))

            results = cursor.fetchall()

            elapsed = time.time() - start_time
            if elapsed > 0.1:
                self.logger.debug(f"get_latest_snapshots took {elapsed:.3f} sec")

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
                self.logger.warning("⚠ Table snapshots_meta does not exist, create data snapshots")
                return []
            raise
        finally:
            conn.close()

    def cleanup_old_data(self, retention_hours: int = 24, cleanup_colors: bool = False):
        """
        Clean up old data with retention period control
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cutoff_time = (datetime.now(tz=timezone.utc) - timedelta(hours=retention_hours)).isoformat()

            self.logger.debug(f"🧹 Cleaning data older than {retention_hours} hours (up to {cutoff_time})...")

            # 1. Delete old tracks by created_at
            cursor.execute('DELETE FROM tracks WHERE created_at < ?', (cutoff_time,))
            tracks_deleted = cursor.rowcount
            if tracks_deleted > 0:
                self.logger.debug(f"🗑️ Deleted {tracks_deleted} old tracks")

            # 2. Delete old snapshots
            # Find old tables
            cursor.execute('''
                SELECT table_name FROM snapshots_meta 
                WHERE created_at < ?
            ''', (cutoff_time,))

            old_tables = [row[0] for row in cursor.fetchall()]

            # Delete old tables
            deleted_count = 0
            for table in old_tables:
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {table}")
                    deleted_count += 1
                except Exception as e:
                    self.logger.warning(f"⚠ Error deleting table {table}: {e}")

            # Delete metadata
            cursor.execute('DELETE FROM snapshots_meta WHERE created_at < ?', (cutoff_time,))

            # 3. Clean inactive colors if needed
            if cleanup_colors:
                # Delete inactive pairs from used_pairs that haven't appeared for a long time
                cursor.execute('''
                    DELETE FROM used_pairs 
                    WHERE last_seen < ?
                ''', (cutoff_time,))

                # Delete pair colors that are no longer used
                cursor.execute('''
                    DELETE FROM pair_colors 
                    WHERE pair NOT IN (
                        SELECT DISTINCT pair FROM used_pairs
                    ) AND is_system = 0
                ''')

                self.logger.info(f"🗑️ Cleaned inactive colors")

            conn.commit()

            if deleted_count > 0:
                self.logger.debug(f"✅ Deleted {deleted_count} old snapshots")

        except Exception as e:
            self.logger.error(f"❌ Error cleaning data: {e}")
            conn.rollback()
        finally:
            conn.close()

    def get_all_tables(self) -> List[str]:
        """Get list of all tables (for debugging)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            return [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()

    def get_snapshot_count(self, exchange: str, market_type: str) -> int:
        """Get number of snapshots"""
        conn = sqlite3.connect(self.db_path)

        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT COUNT(*) FROM snapshots_meta 
                WHERE exchange = ? AND market_type = ?
            ''', (exchange, market_type))

            result = cursor.fetchone()
            return result[0] if result else 0

        except sqlite3.OperationalError:
            return 0
        finally:
            conn.close()

    def clear_all_data(self, keep_colors: bool = True, keep_settings: bool = True):
        """
        Full database cleanup

        Args:
            keep_colors: Keep color table
            keep_settings: Keep user settings
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            self.logger.info("🧹 Starting full database cleanup...")

            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            all_tables = [row[0] for row in cursor.fetchall()]

            tables_to_keep = []
            if keep_colors:
                tables_to_keep.extend(['pair_colors', 'used_pairs'])
            if keep_settings:
                tables_to_keep.append('user_settings')

            tables_to_delete = [t for t in all_tables if t not in tables_to_keep]

            deleted_count = 0
            for table in tables_to_delete:
                try:
                    # Skip system tables
                    if table.startswith('sqlite_'):
                        continue

                    cursor.execute(f"DROP TABLE IF EXISTS {table}")
                    deleted_count += 1
                    self.logger.info(f"🗑️ Deleted table: {table}")
                except Exception as e:
                    self.logger.warning(f"⚠ Error deleting table {table}: {e}")

            # If snapshots_meta was deleted, recreate it
            if 'snapshots_meta' in tables_to_delete:
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS snapshots_meta (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        table_name TEXT UNIQUE NOT NULL,
                        exchange TEXT NOT NULL,
                        market_type TEXT NOT NULL,
                        exchange_timestamp TIMESTAMP NOT NULL,
                        created_at TIMESTAMP NOT NULL,
                        period_minutes INTEGER NOT NULL,
                        row_count INTEGER NOT NULL,
                        FOREIGN KEY (table_name) REFERENCES sqlite_master(name) ON DELETE CASCADE
                    )
                ''')

                # Restore indexes
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_snapshots_exchange_time 
                    ON snapshots_meta(exchange_timestamp)
                ''')
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_snapshots_created_at 
                    ON snapshots_meta(created_at)
                ''')
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_snapshots_exchange_market 
                    ON snapshots_meta(exchange, market_type)
                ''')

            conn.commit()
            self.logger.info(f"✅ Database cleared. Deleted tables: {deleted_count}")

        except Exception as e:
            self.logger.error(f"❌ Error clearing database: {e}")
            conn.rollback()
        finally:
            conn.close()

    def save_setting(self, key: str, value: Any):
        """Save setting"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # Convert value to string
            if isinstance(value, (list, dict)):
                value_str = json.dumps(value)
            else:
                value_str = str(value)

            cursor.execute('''
                INSERT OR REPLACE INTO user_settings (setting_key, setting_value, last_updated)
                VALUES (?, ?, ?)
            ''', (key, value_str, datetime.now().isoformat()))

            conn.commit()

        except Exception as e:
            self.logger.error(f"❌ Error saving setting: {e}")
            conn.rollback()
        finally:
            conn.close()

    def get_setting(self, key: str, default: Any = None) -> Any:
        """Get setting"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT setting_value FROM user_settings WHERE setting_key = ?
            ''', (key,))

            result = cursor.fetchone()
            if result:
                value = result[0]
                # Try to decode JSON
                try:
                    return json.loads(value)
                except:
                    return value
            return default

        except Exception as e:
            self.logger.warning(f"⚠ Error getting setting: {e}")
            return default
        finally:
            conn.close()

    def get_all_settings(self) -> Dict[str, Any]:
        """Get all settings"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute('SELECT setting_key, setting_value FROM user_settings')
            results = cursor.fetchall()

            settings = {}
            for key, value in results:
                try:
                    settings[key] = json.loads(value)
                except:
                    settings[key] = value

            return settings

        except Exception as e:
            self.logger.warning(f"⚠ Error getting settings: {e}")
            return {}
        finally:
            conn.close()

    def get_or_create_pair_color(self, pair: str) -> Tuple[Optional[int], Optional[str]]:
        """Get or create color for a pair with TTL caching"""
        current_time = time.time()

        # Check cache (TTL = 300 seconds = 5 minutes)
        if (hasattr(self, '_pair_color_cache') and
                hasattr(self, '_pair_color_cache_ttl') and
                hasattr(self, '_pair_color_cache_time') and
                pair in self._pair_color_cache and
                current_time - self._pair_color_cache_time[pair] < self._pair_color_cache_ttl):
            self.logger.debug(f"🔍 Color for {pair} taken from cache")
            return self._pair_color_cache[pair]

        self.logger.debug(f"🔍 Getting color for pair: {pair} from DB")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # Check existing color
            cursor.execute('SELECT id, color FROM pair_colors WHERE pair = ? AND is_system = 0', (pair,))
            result = cursor.fetchone()

            if result:
                color_id, color_hex = result
                # Update cache
                if not hasattr(self, '_pair_color_cache'):
                    self._pair_color_cache = {}
                    self._pair_color_cache_time = {}
                self._pair_color_cache[pair] = (color_id, color_hex)
                self._pair_color_cache_time[pair] = current_time
                return color_id, color_hex

            # Generate new unique color
            new_color = self._generate_unique_color()
            self.logger.debug(f"Created new color for {pair}: {new_color}")
            cursor.execute('''
                INSERT INTO pair_colors (pair, color) 
                VALUES (?, ?)
            ''', (pair, new_color))

            color_id = cursor.lastrowid
            conn.commit()

            # Update cache
            if not hasattr(self, '_pair_color_cache'):
                self._pair_color_cache = {}
                self._pair_color_cache_time = {}
            self._pair_color_cache[pair] = (color_id, new_color)
            self._pair_color_cache_time[pair] = current_time

            self.logger.debug(f"Color saved for {pair}, ID: {color_id}")
            return color_id, new_color

        except Exception as e:
            self.logger.error(f"❌ Error creating color for pair {pair}: {e}")
            conn.rollback()
            return None, None
        finally:
            conn.close()


    def invalidate_pair_color_cache(self, pair: str = None):
        """Invalidate color cache"""
        if pair:
            # Invalidate cache for a specific pair
            if hasattr(self, '_pair_color_cache') and pair in self._pair_color_cache:
                del self._pair_color_cache[pair]
            if hasattr(self, '_pair_color_cache_time') and pair in self._pair_color_cache_time:
                del self._pair_color_cache_time[pair]
            self.logger.info(f"🗑️ Color cache for {pair} invalidated")
        else:
            # Invalidate entire cache
            if hasattr(self, '_pair_color_cache'):
                self._pair_color_cache.clear()
            if hasattr(self, '_pair_color_cache_time'):
                self._pair_color_cache_time.clear()
            self.logger.info("🗑️ Entire color cache invalidated")

    def get_snapshot_data(self, table_name: str) -> pd.DataFrame:
        """Get data from a snapshot"""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            if not cursor.fetchone():
                return pd.DataFrame()

            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

            # Try to convert timestamp column to datetime if present
            if 'timestamp' in df.columns:
                try:
                    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
                except:
                    # If failed, leave as is
                    pass

            return df
        except Exception as e:
            self.logger.error(f"⚠ Error reading table {table_name}: {e}")
            return pd.DataFrame()
        finally:
            conn.close()

    def verify_and_fix_snapshot_colors(self):
        """Check and fix colors in snapshots"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            self.logger.debug("🔍 Checking color integrity in snapshots...")

            # Get all snapshots
            cursor.execute('SELECT table_name FROM snapshots_meta')
            snapshots = [row[0] for row in cursor.fetchall()]

            fixed_count = 0

            for table_name in snapshots:
                # Check table structure
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [col[1] for col in cursor.fetchall()]

                # Fix colour
                if 'colour' in columns:
                    cursor.execute(f"""
                        SELECT DISTINCT colour FROM {table_name} 
                        WHERE colour IS NOT NULL AND colour != ''
                    """)
                    colors = cursor.fetchall()

                    for (color_val,) in colors:
                        fixed = self._fix_color_value(cursor, table_name, 'colour', color_val)
                        if fixed:
                            fixed_count += 1

                # Fix manual_colour
                if 'manual_colour' in columns:
                    cursor.execute(f"""
                        SELECT DISTINCT manual_colour FROM {table_name} 
                        WHERE manual_colour IS NOT NULL AND manual_colour != ''
                    """)
                    manual_colors = cursor.fetchall()

                    for (manual_color,) in manual_colors:
                        fixed = self._fix_color_value(cursor, table_name, 'manual_colour', manual_color)
                        if fixed:
                            fixed_count += 1

            conn.commit()
            self.logger.debug(f"✅ Fixed {fixed_count} color entries")

        except Exception as e:
            self.logger.error(f"❌ Error checking colors: {e}")
            conn.rollback()
        finally:
            conn.close()

    def clear_tracks_table(self):
        """Clear tracks table"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute('DROP TABLE IF EXISTS tracks')
            self.logger.debug("✅ Tracks table cleared")

            # Recreate table with correct structure
            self.create_tracks_table()
            self.logger.debug("✅ Tracks table recreated")
        except Exception as e:
            self.logger.error(f"❌ Error clearing tracks table: {e}")
            conn.rollback()
        finally:
            conn.close()

    def delete_tracks_for_exchange(self, exchange: str, market_type: str):
        """Delete all tracks for specified exchange and market type"""
        conn = sqlite3.connect(self.db_path)
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

    def _fix_color_value(self, cursor, table_name: str, column: str, value: Any) -> bool:
        """Fix color value in column"""
        try:
            if value is None:
                return False

            # If it's a number (color ID)
            if isinstance(value, (int, float)):
                color_id = int(value)
                cursor.execute('SELECT color FROM pair_colors WHERE id = ?', (color_id,))
                color_result = cursor.fetchone()
                if color_result:
                    hex_color = color_result[0]
                    cursor.execute(f"""
                        UPDATE {table_name} 
                        SET {column} = ? 
                        WHERE {column} = ?
                    """, (hex_color, value))
                    return True

            # If it's a string but not hex color
            elif isinstance(value, str) and not value.startswith('#') and value.isdigit():
                try:
                    color_id = int(value)
                    cursor.execute('SELECT color FROM pair_colors WHERE id = ?', (color_id,))
                    color_result = cursor.fetchone()
                    if color_result:
                        hex_color = color_result[0]
                        cursor.execute(f"""
                            UPDATE {table_name} 
                            SET {column} = ? 
                            WHERE {column} = ?
                        """, (hex_color, value))
                        return True
                except ValueError:
                    pass

        except Exception as e:
            self.logger.warning(f"Error fixing color {value} in {table_name}.{column}: {e}")

        return False

    def create_tracks_table(self):
        """Create tracks table if it does not exist"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tracks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pair TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    market_type TEXT NOT NULL,
                    track_data TEXT NOT NULL,
                    last_highlighted_time TIMESTAMP,  -- Time of last highlighted point in track
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create indexes for faster queries
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_tracks_pair_exchange 
                ON tracks(pair, exchange, market_type)
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_tracks_created_at 
                ON tracks(created_at)
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_tracks_last_highlighted 
                ON tracks(last_highlighted_time)
            ''')

            conn.commit()
            self.logger.debug("✅ Tracks table created/verified")

        except Exception as e:
            self.logger.error(f"❌ Error creating tracks table: {e}")
            conn.rollback()
        finally:
            conn.close()