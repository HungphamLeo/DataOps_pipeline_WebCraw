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

    def configure_from_project_config(self, project_config_path: str) -> None:
        """Load logger routing from project YAML config and merge into current config."""
        try:
            with open(project_config_path, 'r', encoding='utf-8') as f:
                project_config = yaml.safe_load(f) or {}
        except Exception:
            return

        project_logger = project_config.get('project_params', {}).get('logger', {})
        if not isinstance(project_logger, dict):
            return

        root_level = project_logger.get('level')
        if root_level and isinstance(root_level, str):
            self.config.setdefault('loggers', {}).setdefault('root', {})['level'] = root_level.upper()

        def _build_section(prefix: str, node: Any):
            if not isinstance(node, dict):
                return
            if 'files' in node and isinstance(node['files'], dict):
                logger_name = f"logger{prefix}"
                logger_level = node.get('level', project_logger.get('level', 'INFO')).upper()
                handlers = []
                storage_path = node.get('storage_path', 'shared/logger/logs')
                for level_name, filename in node['files'].items():
                    handler_name = f"{logger_name}.{level_name}"
                    handlers.append(handler_name)
                    self.config.setdefault('handlers', {})[handler_name] = {
                        'class': 'logging.handlers.RotatingFileHandler',
                        'filename': str(Path(storage_path) / filename),
                        'maxBytes': node.get('max_size_mb', 10485760) * 1024 * 1024 if node.get('max_size_mb') else 10485760,
                        'backupCount': node.get('backup_count', 3),
                        'formatter': 'default',
                        'level': level_name.upper(),
                    }
                self.config.setdefault('loggers', {})[logger_name] = {
                    'level': logger_level,
                    'handlers': handlers,
                    'propagate': False,
                }
                return
            for key, value in node.items():
                _build_section(f".{key}", value)

        for section_name in ['ingestion_log', 'storage_log', 'processing_log', 'governance_log']:
            section = project_logger.get(section_name)
            _build_section(f".{section_name}", section)

        self._ensure_handler_dirs()
        logging.config.dictConfig(self.config)

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
