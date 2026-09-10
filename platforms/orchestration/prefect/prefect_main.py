"""
Module: platforms.orchestration.prefect.prefect_main
Layer: Platform Orchestration Subsystem - Prefect
Responsibility: Configuration management and logger orchestration adapter for Prefect flows,
                providing abstract protocols for configuration loading and logger resolution.
Does NOT contain: Direct print statements, hardcoded pipeline business logic, source-specific mutations.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, Optional
import logging
import yaml

from shared.logger.python_main_logger import logger_manager


class ConfigLoader(ABC):
    """Abstract Base Class for loading pipeline configuration dictionaries."""
    @abstractmethod
    def load(self, config_path: Optional[str]) -> Dict[str, Any]:
        """Load configuration from path or environment."""
        pass


class FileConfigLoader(ConfigLoader):
    """File-based YAML configuration loader."""
    def load(self, config_path: Optional[str]) -> Dict[str, Any]:
        if not config_path:
            return {}
        path = Path(config_path)
        if not path.exists():
            return {}
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}


class LoggerFactory(ABC):
    """Abstract Base Class for logger creation in orchestration layer."""
    @abstractmethod
    def get_logger(self, name: str) -> logging.Logger:
        """Retrieve configured logger by name."""
        pass


class DefaultLoggerFactory(LoggerFactory):
    """Default logger factory using platform logger_manager."""
    def get_logger(self, name: str) -> logging.Logger:
        return logger_manager.get_logger(name)


class PrefectETLPipelineConfig:
    """
    Centralized configuration manager for ETL pipelines running under Prefect.
    - Single Responsibility: Exposes configuration access + lazy logger resolution.
    - Dependency Injection: Accepts ConfigLoader and LoggerFactory for clean testability.
    - Production Grade: No print statements; structured logging only.
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        config_loader: Optional[ConfigLoader] = None,
        logger_factory: Optional[LoggerFactory] = None,
    ):
        self._config_path = config_path
        self._loader = config_loader or FileConfigLoader()
        self._logger_factory = logger_factory or DefaultLoggerFactory()
        self._loggers: Dict[str, logging.Logger] = {}
        self._internal_logger = self._logger_factory.get_logger("logger.prefect")
        self._config: Dict[str, Any] = {}
        self._load_config()

    def _load_config(self) -> None:
        try:
            self._internal_logger.debug(f"Loading Prefect configuration from: {self._config_path}")
            cfg = self._loader.load(self._config_path)
            self._config = cfg.get("project_params", cfg)
        except Exception as exc:
            self._config = {}
            self._internal_logger.error(f"Failed to load Prefect pipeline configuration: {str(exc)}", exc_info=True)

    # --- Logger helpers (lazy) ---
    def _get_logger(self, key: str) -> logging.Logger:
        if key not in self._loggers:
            self._loggers[key] = self._logger_factory.get_logger(key)
        return self._loggers[key]

    @property
    def ingestion_logger(self) -> logging.Logger:
        return self._get_logger("logger.ingestion")

    @property
    def bronze_logger(self) -> logging.Logger:
        return self._get_logger("logger.bronze")

    @property
    def silver_logger(self) -> logging.Logger:
        return self._get_logger("logger.silver")

    @property
    def gold_logger(self) -> logging.Logger:
        return self._get_logger("logger.gold")

    @property
    def serving_logger(self) -> logging.Logger:
        return self._get_logger("logger.serving")

    @property
    def dbt_logger(self) -> logging.Logger:
        return self._get_logger("logger.dbt")

    @property
    def data_quality_logger(self) -> logging.Logger:
        return self._get_logger("logger.data_quality")

    # ========== Accessors & convenience ==========

    @property
    def config(self) -> Dict[str, Any]:
        return self._config or {}

    def reload(self) -> None:
        """Force re-load config from source."""
        self._load_config()
        self._loggers.clear()

    def get_environment(self) -> str:
        return self.config.get("environment", "development")

    def is_production(self) -> bool:
        return self.get_environment() == "production"

    def is_development(self) -> bool:
        return self.get_environment() == "development"

    def get_postgres_config(self) -> Dict[str, Any]:
        return self.config.get("storage", {}).get("postgreSQL", {})

    def get_postgresql_schema_dw(self) -> str:
        return self.get_postgres_config().get("dimensions", "public")
