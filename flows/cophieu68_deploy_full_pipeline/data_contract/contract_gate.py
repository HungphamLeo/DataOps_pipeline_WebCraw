from collections import defaultdict
from typing import Any, Dict, List, Literal, Type

from pydantic import BaseModel, ValidationError


class ContractViolationError(Exception):
    """Raise khi check_batch(mode="fail") phát hiện vi phạm."""

    def __init__(self, report: Dict[str, Any]) -> None:
        super().__init__(
            f"contract_violation contract={report['contract']} "
            f"invalid_rows={report['invalid_rows']}/{report['rows']}"
        )
        self.report = report


def check_batch(model: Type[BaseModel], records: List[Dict[str, Any]], *,
                name: str, logger, context: Dict[str, Any] | None = None,
                mode: Literal["warn", "fail"] = "warn") -> Dict[str, Any]:
    """Kiểm tra batch theo contract.

    mode="warn": ghi log, không raise, không lọc dữ liệu.
    mode="fail": ghi log rồi raise ContractViolationError (Prefect task FAILED).
    """
    failures: Dict[tuple, Dict[str, Any]] = defaultdict(lambda: {"count": 0, "samples": []})
    invalid = 0
    for rec in records:
        try:
            model.model_validate(rec)
        except ValidationError as exc:
            invalid += 1
            for e in exc.errors(include_url=False):
                f = failures[(".".join(map(str, e["loc"])), e["type"])]
                f["count"] += 1
                if len(f["samples"]) < 3:
                    f["samples"].append(repr(e.get("input")))

    n = len(records)
    report = {
        "contract": name, "rows": n, "invalid_rows": invalid,
        "failures": [{"field": k[0], "rule": k[1], **v} for k, v in failures.items()],
        **(context or {}),
    }
    if invalid:
        logger.warning("contract_violation %s", report)
        if mode == "fail":
            raise ContractViolationError(report)
    else:
        logger.info("contract_ok contract=%s rows=%d %s", name, n, context or {})
    return report