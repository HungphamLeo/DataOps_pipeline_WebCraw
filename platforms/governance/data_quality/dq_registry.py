"""
Module: platforms.governance.data_quality.dq_registry
Layer: Platform Governance Subsystem - Data Quality
Responsibility: Centralized Data Quality Rule Registry enabling Open-Closed Principle (OCP).
                Allows registering, retrieving, and grouping DQ rules by table or DAMA dimension.
Does NOT contain: Pipeline business logic, storage engines, direct data mutation.
"""

from __future__ import annotations

from typing import Dict, List, Optional
from collections import defaultdict
import threading

from platforms.governance.data_quality.dq_dimensions import (
    DQDimension,
    DataQualityRule,
    DataQualityRuleSet,
)
from shared.logger.python_main_logger import logger_manager


class RuleRegistry:
    """
    Thread-safe Singleton Registry for Data Quality rules across platform domains.
    Supports Open-Closed Principle (OCP): new rules can be registered without modifying core engine.
    """
    _instance: Optional[RuleRegistry] = None
    _lock = threading.Lock()

    def __new__(cls) -> RuleRegistry:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._table_rules = defaultdict(list)
                    cls._instance._dimension_rules = defaultdict(list)
                    cls._instance._rules_by_id = {}
        return cls._instance

    def register(self, table_name: str, rule: DataQualityRule) -> None:
        """
        Register a DataQualityRule for a specific logical/physical table.
        """
        logger = logger_manager.get_logger("logger.data_quality")
        with self._lock:
            # Check duplicate rule ID
            if rule.rule_id in self._rules_by_id:
                logger.warning(f"Overwriting existing rule ID '{rule.rule_id}' in RuleRegistry.")

            self._rules_by_id[rule.rule_id] = rule
            self._table_rules[table_name].append(rule)
            self._dimension_rules[rule.dimension].append(rule)
            logger.debug(f"Registered rule '{rule.rule_id}' (dim: {rule.dimension.value}) for table '{table_name}'.")

    def register_ruleset(self, table_name: str, ruleset: DataQualityRuleSet) -> None:
        """
        Register all rules from a DataQualityRuleSet for a specific table.
        """
        for rule in ruleset.rules:
            self.register(table_name, rule)

    def get_rules_for_table(self, table_name: str) -> List[DataQualityRule]:
        """
        Get all rules registered for a table.
        """
        with self._lock:
            return list(self._table_rules.get(table_name, []))

    def get_ruleset_for_table(self, table_name: str) -> DataQualityRuleSet:
        """
        Construct a DataQualityRuleSet composed of all registered rules for a table.
        """
        rules = self.get_rules_for_table(table_name)
        return DataQualityRuleSet(name=f"ruleset_{table_name}", rules=rules)

    def get_by_dimension(self, dimension: DQDimension) -> List[DataQualityRule]:
        """
        Get all rules across tables belonging to a specific DAMA dimension.
        """
        with self._lock:
            return list(self._dimension_rules.get(dimension, []))

    def get_rule(self, rule_id: str) -> Optional[DataQualityRule]:
        """
        Retrieve a specific rule by ID.
        """
        with self._lock:
            return self._rules_by_id.get(rule_id)

    def clear(self) -> None:
        """
        Clear all registered rules (primarily for testing and dynamic teardown).
        """
        with self._lock:
            self._table_rules.clear()
            self._dimension_rules.clear()
            self._rules_by_id.clear()


# Global Singleton Registry Instance
rule_registry = RuleRegistry()
