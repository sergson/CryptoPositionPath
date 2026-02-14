#logger.py
"""
Logging module with configurable levels
"""
import logging
import os
from datetime import datetime
from typing import Optional
import json


class PerformanceLogger:
    """Performance logger with configurable levels"""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._initialized = True
        self._loggers = {}
        self._log_dir = "logs"
        self._default_level = logging.INFO

        # Create logs directory
        os.makedirs(self._log_dir, exist_ok=True)

        # Default settings
        self.settings = {
            'render_level': 'INFO',       # Level for rendering
            'db_level': 'INFO',            # Level for database
            'analytics_level': 'INFO',     # Level for analytics
            'collector_level': 'INFO',     # Level for collector
            'config_level': 'INFO',        # Level for configuration
            'fetcher_level': 'INFO',       # Level for data fetching
            'performance_log': True         # Enable performance logging
        }

        # Now it's EMPTY here - we don't load from DB in __init__
        # because there is no access to storage

    def initialize_with_storage(self, storage):
        """
        Initialize by loading settings from the database
        This method must be called explicitly after storage is created
        """
        try:
            # Try to load settings from the database
            saved = storage.get_setting('logging_settings')
            if saved:
                if isinstance(saved, str):
                    loaded = json.loads(saved)
                else:
                    loaded = saved

                # Update settings with loaded values
                self.settings.update(loaded)
                print(f"✅ Logging settings loaded from DB: {self.settings}")
            else:
                print(f"⚠ Logging settings not found in DB, using defaults")

        except Exception as e:
            print(f"❌ Error loading logging settings: {e}")
            # Use default settings

        return self

    def setup_logger(self, name: str, log_file: str, level: str = 'INFO'):
        """Configure a logger"""
        # Convert level string to logging constant
        level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        log_level = level_map.get(level.upper(), logging.INFO)

        # Create logger
        logger = logging.getLogger(name)
        logger.setLevel(log_level)

        # Remove existing handlers
        logger.handlers.clear()

        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s.%(msecs)03d [%(levelname)-8s] %(name)s - %(message)s',
            datefmt='%H:%M:%S'
        )

        # File handler
        log_path = os.path.join(self._log_dir, log_file)
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)

        # Add handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        self._loggers[name] = logger
        return logger

    def get_logger(self, name: str, module_type: str = 'render'):
        """Get a logger with current level settings"""
        log_file = f"{module_type}_{datetime.now().strftime('%Y%m%d')}.log"

        # Get level from settings
        level_key = f'{module_type}_level'
        level = self.settings.get(level_key, self._default_level)

        if name in self._loggers:
            # Update level if changed
            for handler in self._loggers[name].handlers:
                handler.setLevel(logging.getLevelName(level))
            self._loggers[name].setLevel(logging.getLevelName(level))
            return self._loggers[name]

        return self.setup_logger(name, log_file, level)

    def update_settings(self, settings: dict):
        """Update logging settings"""
        self.settings.update(settings)

        # Reconfigure existing loggers
        for name, logger in self._loggers.items():
            # Determine module type from logger name
            module_type = 'render' if 'render' in name.lower() else \
                'db' if 'db' in name.lower() else \
                    'analytics' if 'analytics' in name.lower() else \
                        'collector' if 'collector' in name.lower() else 'render'

            level_key = f'{module_type}_level'
            level = self.settings.get(level_key, self._default_level)

            logger.setLevel(logging.getLevelName(level))
            for handler in logger.handlers:
                handler.setLevel(logging.getLevelName(level))

    def save_settings(self, storage):
        """Save settings to the database"""
        try:
            storage.save_setting('logging_settings', json.dumps(self.settings))
        except Exception as e:
            print(f"❌ Error saving logging settings: {e}")

    def load_settings(self, storage):
        """Load settings from the database"""
        try:
            saved = storage.get_setting('logging_settings')
            if saved and isinstance(saved, str):
                loaded = json.loads(saved)
                self.update_settings(loaded)
        except Exception as e:
            print(f"⚠ Error loading logging settings: {e}")


# Singleton instance
perf_logger = PerformanceLogger()