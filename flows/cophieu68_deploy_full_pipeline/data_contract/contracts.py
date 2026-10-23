from typing import Annotated, Any, Literal
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, field_validator
from flows.cophieu68_deploy_full_pipeline.ingestion.dto.extract_models import (
    _NULL_SENTINELS, norm_date, norm_numeric,
)

class StockPriceContract(BaseModel):
    """bronze.stock_prices: giao dịch theo ngày của một mã cổ phiếu (nguồn cophieu68.vn)."""
    model_config = ConfigDict(extra="allow")   # bronze additive: cột thừa không lỗi

    symbol: str = Field(
        pattern=r"^[A-Z0-9]{2,10}$",
        description="Mã cổ phiếu niêm yết, viết hoa. Ví dụ: HPG, VNM.")
    date: str = Field(
        description="Ngày giao dịch. Chấp nhận YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY, YYYY/MM/DD.")
    close_price: float = Field(
        gt=0, description="Giá đóng cửa của phiên (nghìn VND, theo nguồn). Phải > 0.")
    open_price: float = Field(ge=0, description="Giá mở cửa của phiên.")
    high_price: float = Field(ge=0, description="Giá cao nhất trong phiên.")
    low_price: float = Field(ge=0, description="Giá thấp nhất trong phiên.")
    volume: int = Field(ge=0, description="Tổng khối lượng khớp trong phiên (cổ phiếu).")
    foreign_buy: int = Field(ge=0, description="Khối lượng khối ngoại mua.")
    foreign_sell: int = Field(ge=0, description="Khối lượng khối ngoại bán.")
    foreign_value: float = Field(description="Giá trị giao dịch khối ngoại.")

    @field_validator("date")
    @classmethod
    def _date_parsable(cls, v: str) -> str:
        if norm_date(v) is None:
            raise ValueError(f"unparsable date: {v!r}")
        return v

# ── Validators dùng chung ────────────────────────────────────────────────────
# Giá trị rỗng/"-"/"None" được coi là null hợp lệ (nguồn là HTML scrape).
# Chỉ báo lỗi khi có giá trị thật nhưng không parse được.

def _numeric_or_null(v: Any) -> Any:
    if v is None or (isinstance(v, (int, float)) and not isinstance(v, bool)):
        return v
    text = str(v).strip().replace("\u00a0", "")
    if text in _NULL_SENTINELS:
        return v
    if norm_numeric(text) is None:
        raise ValueError(f"not numeric: {v!r}")
    return v


def _date_or_null(v: Any) -> Any:
    if v is None or str(v).strip() in _NULL_SENTINELS:
        return v
    if norm_date(v) is None:
        raise ValueError(f"unparsable date: {v!r}")
    return v


NumLike = Annotated[Any, AfterValidator(_numeric_or_null)]
DateLike = Annotated[Any, AfterValidator(_date_or_null)]

_SYMBOL_PATTERN = r"^[A-Z0-9]{2,10}$"


class _BronzeContract(BaseModel):
    """Bronze additive: cột thừa không lỗi."""
    model_config = ConfigDict(extra="allow")


# ── Per-symbol contracts ─────────────────────────────────────────────────────

class CompanyProfileContract(_BronzeContract):
    """bronze.company_profile → staging.company_profile → normalized.dim_company."""
    symbol: str = Field(pattern=_SYMBOL_PATTERN)
    current_price: NumLike = None
    established_date: DateLike = None
    listed_date: DateLike = None
    listed_volume_initial: NumLike = None
    listed_volume: NumLike = None
    circulating_volume: NumLike = None
    market_capitalization: NumLike = None


class FinancialRatiosContract(_BronzeContract):
    """bronze.financial_ratios → staging.financial_ratios."""
    symbol: str = Field(pattern=_SYMBOL_PATTERN)
    reference_price: NumLike = None
    open_price: NumLike = None
    high_price: NumLike = None
    low_price: NumLike = None
    volume: NumLike = None
    book_value: NumLike = None
    eps: NumLike = None
    pe: NumLike = None
    pb: NumLike = None
    roe: NumLike = None
    roa: NumLike = None
    beta: NumLike = None
    market_cap: NumLike = None
    listed_volume: NumLike = None


class BusinessPlanContract(_BronzeContract):
    """bronze.business_plan. `symbol` do BronzeExecutor inject trước khi gate."""
    symbol: str = Field(pattern=_SYMBOL_PATTERN)
    Year: str = Field(pattern=r"^\d{4}")
    Plan_revenue: NumLike = None
    Pass_revenue: NumLike = None
    Plan_profit: NumLike = None
    Pass_profit: NumLike = None


class IncomeStatementContract(_BronzeContract):
    """bronze.income_statement_{quarter,year}. Cột dữ liệu động theo bảng HTML,
    chỉ ràng buộc khóa."""
    symbol: str = Field(pattern=_SYMBOL_PATTERN)
    report_type: Literal["quarter", "year"]


class BalanceSheetContract(IncomeStatementContract):
    """bronze.balance_sheet_{quarter,year}. Cùng ràng buộc khóa."""


class FinancialReportSummaryContract(_BronzeContract):
    """bronze.financial_report_summary. report_type là key tự do từ crawler."""
    symbol: str = Field(pattern=_SYMBOL_PATTERN)
    report_type: str = Field(min_length=1)


# ── Global (reference) contracts ─────────────────────────────────────────────

class IndustrySectorContract(_BronzeContract):
    """bronze.industry_sectors."""
    industry_code: str = Field(min_length=1)
    industry_name: str = Field(min_length=1)
    symbol: str = Field(pattern=_SYMBOL_PATTERN)
    close_price: NumLike = None
    Increase_decrease: NumLike = None
    volumn24h: NumLike = None
    volumn52w: NumLike = None
    listed_volumn: NumLike = None
    market_capitalization: NumLike = None


class MarketTypeContract(_BronzeContract):
    """bronze.market_type_sectors."""
    market_type_code: str = Field(min_length=1)
    market_type_name: str = Field(min_length=1)
    symbol: str = Field(pattern=_SYMBOL_PATTERN)
    close_price: NumLike = None
    Increase_decrease: NumLike = None
    volumn24h: NumLike = None
    volumn52w: NumLike = None
    listed_volumn: NumLike = None
    market_capitalization: NumLike = None