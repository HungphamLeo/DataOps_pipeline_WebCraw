import logging

import pytest

from flows.cophieu68_deploy_full_pipeline.data_contract.contract_gate import (
    ContractViolationError, check_batch,
)
from flows.cophieu68_deploy_full_pipeline.data_contract.contracts import (
    BalanceSheetContract, BusinessPlanContract, CompanyProfileContract,
    FinancialRatiosContract, IndustrySectorContract, MarketTypeContract,
)

LOG = logging.getLogger("test_contract")


def _check(model, rows, **kw):
    return check_batch(model, rows, name="t", logger=LOG, **kw)


def test_company_profile_ok_with_null_sentinels():
    rec = {"symbol": "HPG", "listed_date": "-", "market_capitalization": "1,234.5B", "fax": ""}
    assert _check(CompanyProfileContract, [rec])["invalid_rows"] == 0


def test_company_profile_bad_symbol_and_date():
    rec = {"symbol": "hpg!", "listed_date": "31/31/2020"}
    report = _check(CompanyProfileContract, [rec])
    fields = {f["field"] for f in report["failures"]}
    assert report["invalid_rows"] == 1 and {"symbol", "listed_date"} <= fields


def test_financial_ratios_non_numeric_value_flagged():
    report = _check(FinancialRatiosContract, [{"symbol": "VNM", "pe": "abc"}])
    assert report["invalid_rows"] == 1


def test_business_plan_requires_year():
    assert _check(BusinessPlanContract, [{"symbol": "VNM", "Year": "2024"}])["invalid_rows"] == 0
    assert _check(BusinessPlanContract, [{"symbol": "VNM", "Year": "n/a"}])["invalid_rows"] == 1


def test_balance_sheet_report_type_enum():
    assert _check(BalanceSheetContract, [{"symbol": "FPT", "report_type": "year", "x": 1}])["invalid_rows"] == 0
    assert _check(BalanceSheetContract, [{"symbol": "FPT", "report_type": "month"}])["invalid_rows"] == 1


def test_industry_and_market_require_codes():
    assert _check(IndustrySectorContract, [{"industry_code": "", "industry_name": "x", "symbol": "FPT"}])["invalid_rows"] == 1
    assert _check(MarketTypeContract, [{"market_type_code": "HOSE", "market_type_name": "HOSE", "symbol": "FPT"}])["invalid_rows"] == 0


def test_warn_mode_does_not_raise():
    _check(CompanyProfileContract, [{"symbol": ""}], mode="warn")


def test_fail_mode_raises_with_report():
    with pytest.raises(ContractViolationError) as ei:
        _check(CompanyProfileContract, [{"symbol": ""}], mode="fail")
    assert ei.value.report["invalid_rows"] == 1