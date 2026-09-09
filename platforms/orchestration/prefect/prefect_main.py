from datetime import timedelta
from typing import Dict, Any, Optional
import logging
from platforms.processing.base_processing_subsystem.base_processing import ConfigLoader, FileConfigLoader, LoggerFactory, DefaultLoggerFactory
class PrefectETLPipelineConfig:
    """
    Centralized configuration manager cho ETL pipeline.
    - Single Responsibility: chỉ expose configuration + logger access.
    - Dependency Injection: nhận ConfigLoader và LoggerFactory để dễ test/migrate.
    - Lazy init loggers (chỉ tạo khi cần).
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
        # self._config: Dict[str, Any] = {}
        self._loggers: Dict[str, logging.Logger] = {}
        self._load_config()
        

    def _load_config(self) -> None:
        try:
            print("Loading config from:", self._config_path)
            cfg = self._loader.load(self._config_path)
            self._config = cfg.get('project_params')

        except Exception:
            self._config = {}
            print("self.config is null")

    # --- Logger helpers (lazy) ---
    def _get_logger(self, key: str) -> logging.Logger:
        if key not in self._loggers:
            self._loggers[key] = self._logger_factory.get_logger(key)
        return self._loggers[key]

    @property
    def cophieu68_extract_logger(self) -> logging.Logger:
        return self._get_logger("logger.ingestion_log.cophieu68.extract")

    @property
    def cophieu68_load_logger(self) -> logging.Logger:
        return self._get_logger("logger.ingestion_log.cophieu68.load")

    @property
    def cophieu68_transform_logger(self) -> logging.Logger:
        return self._get_logger("logger.ingestion_log.cophieu68.transform")

    @property
    def storage_mongodb(self) -> logging.Logger:
        return self._get_logger("logger.storage_log.mongodb")

    @property
    def storage_postgresql(self) -> logging.Logger:
        return self._get_logger("logger.storage_log.postgresql")

    # ========== Accessors & convenience ==========

    @property
    def config(self) -> Dict[str, Any]:
        return self._config or {}

    def reload(self) -> None:
        """Force re-load config from source."""
        self._load_config()
        # reset loggers so they can pick new config if needed
        self._loggers = {}

    def get_airflow_default_args(self) -> Dict[str, Any]:
        airflow_config = self.config.get("airflow", {}).get("default_args", {})
        return {
            "owner": airflow_config.get("owner", "data-engineering"),
            "depends_on_past": airflow_config.get("depends_on_past", False),
            "email_on_failure": airflow_config.get("email_on_failure", True),
            "email_on_retry": airflow_config.get("email_on_retry", False),
            "retries": airflow_config.get("retries", 1),
            "retry_delay": timedelta(seconds=airflow_config.get("retry_delay_sec", 300)),
            "execution_timeout": timedelta(seconds=airflow_config.get("execution_timeout_sec", 7200)),
        }

    def get_environment(self) -> str:
        return self.config.get("environment", "development")

    def is_production(self) -> bool:
        return self.get_environment() == "production"

    def is_development(self) -> bool:
        return self.get_environment() == "development"

    def get_monitoring_config(self) -> Dict[str, Any]:
        return self.config.get("monitoring", {})

    # --- Convenience: storage / collections helpers ---
    def get_mongo_config(self) -> Dict[str, Any]:
        return self.config.get("storage", {}).get("mongodb", {})

    def get_postgres_config(self) -> Dict[str, Any]:
        return self.config.get("storage", {}).get("postgreSQL", {})

    def get_mongodb_collection(self, logical_name: str, default: Optional[str] = None) -> Optional[str]:
        cols = self.get_mongo_config().get("collections", {})
        if isinstance(cols, dict):
            v = cols.get(logical_name)
            if isinstance(v, dict):
                return v.get("name", default or logical_name)
            return v or default or logical_name
        # fallback if list style used
        if isinstance(cols, list):
            for item in cols:
                if item.get("id") == logical_name or item.get("name") == logical_name:
                    return item.get("name")
        return default or logical_name

    def get_postgresql_schema_dw(self):
        pg_config = self.get_postgres_config()
        return pg_config.get("dimensions", "public")