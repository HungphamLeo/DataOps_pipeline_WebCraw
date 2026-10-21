"""
logger_setup.py — Centralized Logger Manager cho toàn bộ dataops_batch_api.

Pattern:
    - LoggerManager: singleton, load config từ logger_config.yaml một lần duy nhất.
    - JsonFormatter: serialize log records thành JSON cho ELK / Loki / Grafana.
    - get_logger(): convenience wrapper, cache logger instance theo __name__.

Usage (entrypoint — gọi 1 lần duy nhất):
    from log.logger_setup import setup_pipeline_logging
    setup_pipeline_logging()

Usage (mọi module — chỉ dùng __name__, KHÔNG hardcode tên ngắn):
    import logging
    logger = logging.getLogger(__name__)

    logger.info("msg — key=%s", value)

Logger name = __name__ của module → Python tự route theo hierarchy trong YAML:
    cli.flows.serving.bronze_flow   → khớp logger "cli"
    platforms.processing.delta_utils → khớp logger "platforms.processing"
    ...
Thay đổi routing chỉ cần sửa logger_config.yaml, không đụng vào từng module.

Rules:
    - Dùng %s formatting, KHÔNG dùng f-string trong logger calls.
    - Module-level `logger = logging.getLogger(__name__)`, không import inline.
    - Không gọi logging.basicConfig() ở bất kỳ file nào ngoài logger_setup.py.
"""
from __future__ import annotations

import json
import logging
import logging.config
import logging.handlers
import sys
from pathlib import Path
from typing import Any, Dict, Optional


# ─── JsonFormatter ────────────────────────────────────────────────────────────

class JsonFormatter(logging.Formatter):
    """
    Serialize log records thành valid JSON cho log collectors (ELK / Loki / Grafana).

    Fields chuẩn: time, level, logger, message, module, line, process, thread.
    Extra fields được gắn vào record qua logger.info("msg", extra={...}):
        run_id, component, operation, table, source, target, correlation_id

    correlation_id: UUID dùng để trace 1 job execution end-to-end qua tất cả log entries.
    Cách dùng:
        logger.info("job started", extra={"correlation_id": str(uuid.uuid4())})
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "time":    self.formatTime(record, self.datefmt),
            "level":   record.levelname,
            "logger":  record.name,
            "message": record.getMessage(),
            "module":  record.module,
            "line":    record.lineno,
            "process": record.process,
            "thread":  record.threadName,
        }
        for field in ("run_id", "component", "operation", "table", "source", "target",
                      "correlation_id"):
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


# ─── LoggerManager ────────────────────────────────────────────────────────────

class LoggerManager:
    """
    Singleton quản lý toàn bộ logging configuration cho pipeline.

    - Load dictConfig từ logger_config.yaml một lần duy nhất.
    - Patch filename handlers sang absolute path (tránh phụ thuộc working dir).
    - Cache logger instances để tránh tạo lại.
    - Fallback sang console-only khi YAML không tồn tại hoặc bị lỗi.
    """

    _instance: Optional["LoggerManager"] = None
    _loggers:  Dict[str, logging.Logger] = {}

    def __new__(cls) -> "LoggerManager":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if getattr(self, "_initialized", False):
            return
        # Resolve config path qua config.py nếu có; fallback sang sibling yaml.
        # Import nằm trong try/except vì logger_setup.py có thể được import
        # trước khi dataops_batch_api/ được thêm vào sys.path (ví dụ: chạy từ
        # thư mục ngoài, test runner, hay Prefect worker).
        # Fallback dùng Path(__file__) — luôn resolve đúng bất kể working dir.
        try:
            from config import Paths  # noqa: PLC0415
            self._config_path = Paths.LOGGER_CONFIG_YAML
            self._log_storage = Paths.LOG_STORAGE
        except (ImportError, AttributeError):
            _here = Path(__file__).parent
            self._config_path = _here / "logger_config.yaml"
            self._log_storage = _here.parent.parent / "logs"

    # ── Public API ────────────────────────────────────────────────────────────

    def configure(self) -> None:
        """
        Load logger_config.yaml và áp dụng dictConfig.

        Gọi 1 lần tại entrypoint. Gọi nhiều lần là no-op sau lần đầu thành công.
        Fallback sang console-only nếu YAML không khả dụng — pipeline không crash.
        """
        if self._initialized:
            return

        try:
            self._log_storage.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            self._bootstrap_fallback()
            logging.getLogger(__name__).warning(
                "Cannot create log_storage dir %s: %s — console-only logging",
                self._log_storage, exc,
            )
            self._initialized = True
            return

        if not self._config_path.exists():
            self._bootstrap_fallback()
            logging.getLogger(__name__).warning(
                "logger_config.yaml not found at %s — console-only logging",
                self._config_path,
            )
            self._initialized = True
            return

        try:
            import yaml  # noqa: PLC0415
            with open(self._config_path, "r", encoding="utf-8") as fh:
                cfg: Dict[str, Any] = yaml.safe_load(fh) or {}
            self._patch_file_handler_paths(cfg)
            logging.config.dictConfig(cfg)
            self._initialized = True
            logging.getLogger(__name__).debug(
                "Pipeline logging configured from %s", self._config_path,
            )
        except Exception as exc:
            self._bootstrap_fallback()
            logging.getLogger(__name__).warning(
                "Failed to load logger_config.yaml: %s — console-only logging", exc,
            )
            self._initialized = True

    def get_logger(self, name: str) -> logging.Logger:
        """
        Trả về logger theo name, có cache.

        Args:
            name: Thường là __name__ của module gọi vào.
                  Ví dụ: 'cli.flows.serving.bronze_flow'

        Returns:
            logging.Logger đã được route theo hierarchy trong logger_config.yaml.
        """
        if not self._initialized:
            self.configure()
        if name not in self._loggers:
            self._loggers[name] = logging.getLogger(name)
        return self._loggers[name]

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _bootstrap_fallback(self) -> None:
        """Console-only logging tối thiểu — đảm bảo pipeline không crash."""
        if logging.root.handlers:
            return
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
            stream=sys.stdout,
        )

    def _patch_file_handler_paths(self, cfg: Dict[str, Any]) -> None:
        """
        Thay relative filename trong RotatingFileHandler thành absolute path.

        Giữ nguyên cấu trúc thư mục con (ví dụ logs/bronze/bronze.log → <log_storage>/bronze/bronze.log).
        Tránh phụ thuộc working directory khi chạy pipeline từ các vị trí khác nhau.
        """
        for handler_cfg in cfg.get("handlers", {}).values():
            if not isinstance(handler_cfg, dict):
                continue
            if "filename" in handler_cfg:
                original = Path(handler_cfg["filename"])
                if not original.is_absolute():
                    # Bỏ phần "logs/" prefix nếu có, giữ phần còn lại (bronze/bronze.log)
                    parts = original.parts
                    relative = Path(*parts[1:]) if parts[0] in ("logs", "log") else original
                    abs_path = self._log_storage / relative
                    abs_path.parent.mkdir(parents=True, exist_ok=True)
                    handler_cfg["filename"] = str(abs_path)


# ─── Singleton instance ───────────────────────────────────────────────────────

logger_manager = LoggerManager()


# ─── Convenience functions (public API) ──────────────────────────────────────

def setup_pipeline_logging() -> None:
    """
    Khởi tạo logging từ logger_config.yaml. Gọi 1 lần tại entrypoint.

    Sau lần gọi này, mọi `logging.getLogger(__name__)` trên toàn pipeline
    được route đúng handler theo hierarchy trong logger_config.yaml.
    """
    logger_manager.configure()


def get_logger(name: str) -> logging.Logger:
    """Convenience wrapper: trả về logger theo name (thường là __name__)."""
    return logger_manager.get_logger(name)
