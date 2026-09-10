"""
Module: shared.logger.python_main_logger
Layer: Shared Infrastructure
Responsibility: Centralized Logger Manager implementing Singleton pattern, YAML-driven dictConfig,
                and topic-based pipeline logger creation with structured JSON and context tracking.
Does NOT contain: Pipeline business logic, data processing, storage manipulation.
"""

import logging
import logging.config
from pathlib import Path
from typing import Dict, Any, Optional
import yaml


class RunIdFilter(logging.Filter):
    """Filter that injects run_id into LogRecord if not already provided."""
    def __init__(self, run_id: str = ""):
        super().__init__()
        self.run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "run_id"):
            record.run_id = self.run_id or "N/A"
        return True


class LoggerManager:
    _instance: Optional['LoggerManager'] = None
    _loggers: Dict[str, logging.Logger] = {}

    def __new__(cls, config_path: Optional[str] = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, config_path: Optional[str] = None):
        if self._initialized:
            return
        self.config_path = config_path or Path(__file__).parent / "config" / "logger_config.yaml"
        self.config = self._load_config()
        self._configure_logging()
        self._initialized = True

    def _load_config(self) -> Dict[str, Any]:
        """Load logger config from YAML with fallback default."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        except FileNotFoundError:
            return {
                "version": 1,
                "disable_existing_loggers": False,
                "formatters": {
                    "default": {
                        "format": '{"time":"%(asctime)s", "level":"%(levelname)s", "logger":"%(name)s", "message":"%(message)s", "caller":"%(pathname)s:%(lineno)d", "run_id":"%(run_id)s"}',
                        "datefmt": "%Y-%m-%dT%H:%M:%S"
                    }
                },
                "handlers": {
                    "console": {
                        "class": "logging.StreamHandler",
                        "formatter": "default",
                        "level": "INFO"
                    },
                },
                "loggers": {
                    "root": {
                        "level": "INFO",
                        "handlers": ["console"]
                    }
                }
            }

    def _ensure_handler_dirs(self) -> None:
        """Create directories for any file-based handlers before dictConfig."""
        for handler in self.config.get("handlers", {}).values():
            filename = handler.get("filename")
            if filename:
                Path(filename).parent.mkdir(parents=True, exist_ok=True)

    def _configure_logging(self) -> None:
        """Configure logging from config dictionary and attach default RunIdFilter to handlers."""
        self._ensure_handler_dirs()
        logging.config.dictConfig(self.config)

        # Attach default filter to root/all handlers so format string %(run_id)s never raises KeyError
        default_filter = RunIdFilter(run_id="N/A")
        for handler in logging.root.handlers:
            handler.addFilter(default_filter)

    def get_logger(self, module_name: str) -> logging.Logger:
        """
        Lấy logger cho module cụ thể. Backward-compatible.
        :param module_name: Tên module hoặc logger path (ví dụ: 'logger.bronze', 'platforms.ingestion.extract')
        :return: Logger instance
        """
        if module_name in self._loggers:
            return self._loggers[module_name]

        if module_name.startswith("logger."):
            logger_name = module_name
        else:
            logger_name = f"etl.{module_name.replace('.', '_')}"

        logger = logging.getLogger(logger_name)
        if logger_name not in self.config.get("loggers", {}):
            logger.setLevel(logging.INFO)

        # Add RunIdFilter to ensure %(run_id)s is present
        logger.addFilter(RunIdFilter(run_id="N/A"))

        self._loggers[module_name] = logger
        return logger

    def get_pipeline_logger(self, topic: str, run_id: str = "") -> logging.Logger:
        """
        Lấy pipeline logger cho topic cụ thể gắn run_id vào log context.
        :param topic: Pipeline topic (ví dụ: 'ingestion', 'bronze', 'silver', 'gold', 'serving', 'dbt', 'data_quality', 'prefect')
        :param run_id: Mã định danh của pipeline run
        :return: Logger instance configured for the topic
        """
        logger_name = f"logger.{topic}" if not topic.startswith("logger.") else topic
        cache_key = f"{logger_name}:{run_id}"

        if cache_key in self._loggers:
            return self._loggers[cache_key]

        logger = logging.getLogger(logger_name)
        if logger_name not in self.config.get("loggers", {}):
            logger.setLevel(logging.INFO)

        # Remove existing RunIdFilter if present to avoid duplication
        logger.filters = [f for f in logger.filters if not isinstance(f, RunIdFilter)]
        logger.addFilter(RunIdFilter(run_id=run_id or "N/A"))

        self._loggers[cache_key] = logger
        return logger


# Singleton instance
logger_manager = LoggerManager()
