"""
ingestion/dto/extract_models.py
================================
Data models + value normalizers cho ingestion layer.

Normalizers (_norm_numeric, _norm_date) được dùng ngay tại nguồn để đảm bảo
giá trị đã sạch trước khi đưa vào DQ / Parquet / PostgreSQL.
"""
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Optional

_NULL_SENTINELS = {"", "None", "none", "NULL", "null", "-", "--", "N/A", "n/a"}

_SUFFIX_MAP = {
    "K": Decimal("1e3"),
    "M": Decimal("1e6"),
    "MI": Decimal("1e6"),
    "B": Decimal("1e9"),
    "BI": Decimal("1e9"),
    "T": Decimal("1e12"),
}

_DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d")


def norm_numeric(value) -> Optional[str]:
    """Chuẩn hoá giá trị số về string thập phân. Trả None nếu không parse được.

    Hỗ trợ: dấu phẩy nghìn, ký hiệu đơn vị K/M/B/T/MI/BI, khoảng trắng NBSP,
    cặp số phân cách '#' (lấy trung bình), hậu tố 'x'.
    """
    if value is None:
        return None
    text = str(value).strip().replace("\u00a0", "")
    if text in _NULL_SENTINELS:
        return None
    text = text.replace(",", "").replace("%", "").strip()

    # Cặp "a#b" → trung bình
    if "#" in text:
        parts = [p.strip() for p in text.split("#") if p.strip()]
        if len(parts) == 2:
            a, b = norm_numeric(parts[0]), norm_numeric(parts[1])
            if a is not None and b is not None:
                return format((Decimal(a) + Decimal(b)) / 2, "f")
        text = text.split("#", 1)[0].strip()

    text = re.sub(r"[xX]$", "", text).strip()
    multiplier = Decimal("1")
    m = re.search(r"(MI|BI|K|M|B|T)$", text, re.IGNORECASE)
    if m:
        multiplier = _SUFFIX_MAP[m.group(1).upper()]
        text = text[: -len(m.group(1))].strip()

    if not re.fullmatch(r"[+-]?\d+(?:\.\d+)?", text):
        return None
    try:
        return format(Decimal(text) * multiplier, "f")
    except InvalidOperation:
        return None


def norm_date(value) -> Optional[str]:
    """Chuẩn hoá giá trị ngày về ISO string YYYY-MM-DD. Trả None nếu không parse được."""
    if value is None:
        return None
    text = str(value).strip()
    if text in _NULL_SENTINELS:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None



from dataclasses import dataclass, field, asdict
from typing import List, Dict
import pandas as pd
import json

# ============================================================
# 📌 DATA MODELS
# ============================================================

@dataclass
class StockFinancialReport:
    """Báo cáo tài chính cổ phiếu"""
    symbol: str
    report_type: str
    table_index: int
    data: pd.DataFrame

@dataclass
class IndustrySummaryInfo:
    index: Optional[str] = None
    change: Optional[str] = None
    liquidity: Optional[str] = None
    capital: Optional[str] = None


@dataclass
class IndustryFinancialInfo:
    avg_price: Optional[str] = None
    book_value: Optional[str] = None
    eps: Optional[str] = None
    pe: Optional[str] = None
    roa: Optional[str] = None
    roe: Optional[str] = None


@dataclass
class IndustryCapitalInfo:
    supply_volumn: Optional[str] = None
    total_asset: Optional[str] = None
    total_equity: Optional[str] = None
    total_liabilities: Optional[str] = None
    percentage_debt_on_equity: Optional[str] = None
    percentage_equity_on_assets: Optional[str] = None
    revenue: Optional[str] = None
    profit_before_tax: Optional[str] = None

@dataclass
class BalanceSheet:
    """Bảng cân đối kế toán (tóm tắt)"""
    symbol: str
    report_type: str
    data: pd.DataFrame


@dataclass
class IncomeStatement:
    """Báo cáo kết quả hoạt động / Báo cáo thu nhập (tóm tắt)"""
    symbol: str
    report_type: str
    data: pd.DataFrame

@dataclass
class StockBasicInfo:
    """Thông tin cơ bản cổ phiếu"""
    symbol: str
    company_name: str = ""
    current_price: Optional[float] = None
    price_change: Optional[float] = None
    percent_change: Optional[float] = None
    reference_price: Optional[float] = None
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    volume: Optional[int] = None
    timestamp: str = ""


@dataclass
class StockFinancialRatios:
    """Chỉ tiêu tóm tắt đầu trang"""
    symbol: str
    reference_price: Optional[float] = None
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    volume: Optional[int] = None
    book_value: Optional[str] = None
    eps: Optional[str] = None
    pe: Optional[str] = None
    pb: Optional[str] = None
    roe: Optional[str] = None
    roa: Optional[str] = None
    beta: Optional[float] = None
    market_cap: Optional[str] = None
    listed_volume: Optional[str] = None
    avg_volume_52w: Optional[str] = None
    high_low_52w: Optional[str] = None
    debt: Optional[str] = None
    equity: Optional[str] = None
    debt_to_equity: Optional[str] = None
    equity_to_assets: Optional[str] = None
    cash: Optional[str] = None
    eps_power: Optional[str] = None
    roe_power: Optional[str] = None
    invest_efficiency: Optional[str] = None
    pb_power: Optional[str] = None
    price_growth_power: Optional[str] = None

    def to_df(self) -> pd.DataFrame:
        return pd.DataFrame([asdict(self)])


@dataclass
class DetailsMatchRow:
    """Dòng dữ liệu khớp lệnh"""
    Time_match: str
    Price_match: float
    Increase_decrease: str
    Volume: int
    Accum_volume: int


@dataclass
class DetailsMatchReport:
    """Chi tiết khớp lệnh"""
    symbol: str
    data: list


@dataclass
class BusinessPlanRow:
    """Dòng kế hoạch kinh doanh"""
    Year: str
    Plan_revenue: float
    Pass_revenue: float
    Plan_profit: float
    Pass_profit: float


@dataclass
class BusinessPlanReport:
    """Kế hoạch kinh doanh"""
    symbol: str
    data: pd.DataFrame


@dataclass
class FinancialStatementReport:
    """Báo cáo tài chính (bảng HTML)"""
    symbol: str
    report_type: str
    data: pd.DataFrame


@dataclass
class CompanyProfile:
    """Thông tin công ty"""
    symbol: str
    current_price: str = ""
    full_name: str = ""
    english_name: str = ""
    short_name: str = ""
    address: str = ""
    phone_number: str = ""
    fax: str = ""
    website: str = ""
    email_address: str = ""
    established_date: str = ""
    listed_date: str = ""
    listed_volume_initial: str = ""
    listed_volume: str =""
    circulating_volume: str = ""
    market_capitalization: str = ""
    foreign_buy: str = ""
    foreign_ownership: str = ""


@dataclass
class TradingRecord:
    """Dòng giao dịch"""
    date: str
    close_price: float
    volume: int
    open_price: float
    high_price: float
    low_price: float
    foreign_buy: int
    foreign_sell: int
    foreign_value: float


@dataclass
class TradingData:
    """Dữ liệu giao dịch"""
    symbol: str
    records: List[TradingRecord] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps({
            "symbol": self.symbol,
            "records": [asdict(r) for r in self.records]
        }, ensure_ascii=False, indent=2)


@dataclass
class CompanyBelongToIndustrySector:
    """Ngành nghề công ty"""
    industry_code: str
    industry_name: str
    symbol: str
    company_name:str
    close_price: Optional[float] = None
    Increase_decrease: Optional[float] = None
    volumn24h: Optional[float] = None
    volumn52w: Optional[float] = None
    listed_volumn: Optional[float] = None
    market_capitalization: Optional[float] = None

@dataclass
class CompanyBelongToMarketType:
    """Công ty thuộc loại thị trường"""
    market_type_code: str
    market_type_name: str
    symbol: str
    company_name:str
    close_price: Optional[float] = None
    Increase_decrease: Optional[float] = None
    volumn24h: Optional[float] = None
    volumn52w: Optional[float] = None
    listed_volumn: Optional[float] = None
    market_capitalization: Optional[float] = None


INDUSTRIAL_INFO_TYPE = {"summary_info": 0, "financial_info": 2, "fund_info": 3}

CRAWL_COMPANY_PROFILE_CONFIG = {
    "field_map": {
        "Mã CK" : "symbol",
        "Giá hiện tại" : "current_price",
        "Tên công ty": "full_name",
        "Tên giao dịch": "english_name",
        "tên viết tắt": "short_name",
        "Địa chỉ": "address",
        "Điện thoại": "phone_number",
        "Fax": "fax",
        "Website": "website",
        "Email": "email_address",
        "Ngày thành lập": "established_date",
        "Ngày niêm yết": "listed_date",
        "KL niêm yết lần đầu": "listed_volume_initial",
        "KL niêm yết": "listed_volume",
        "KL lưu hành": "circulating_volume",
        "Vốn thị trường": "market_capitalization",
        "NN mua": "foreign_buy",
        "NN sở hữu": "foreign_ownership"
    }
}
CRAWL_MARKET_LIST_CONFIG = { "VNINDEX" : "vnindex", 
                            "HNX": "hastc", 
                            "UPCOM": "upcom", 
                            "VN30": "vn30" }

MAPPING_INDUSTRY_CODE = {
    "Bán buôn": "^bb",
    "Bất động sản": "^bds",
    "Bảo hiểm": "^bh",
    "Bán lẻ": "^bl",
    "Chế biến Thủy sản": "^cbts",
    "Chứng khoán": "^ck",
    "Công nghệ và Thông tin": "^cntt",
    "Chăm sóc sức khỏe": "^cssk",
    "Dịch vụ lưu trú, ăn uống, giải trí": "^dvltaugt",
    "Dịch vụ tư vấn, hỗ trợ": "^dvtvht",
    "Khai khoáng": "^kk",
    "Ngân hàng": "^nh",
    "Nông - Lâm - Ngư nghiệp": "^nln",
    "Sản phẩm cao su": "^spcs",
    "Sản xuất Hàng gia dụng": "^sxhgd",
    "Sản xuất Nhựa - Hóa chất": "^sxnhc",
    "Sản xuất Phụ trợ": "^sxpt",
    "Sản xuất Thiết bị, máy móc": "^sxtbmm",
    "Thiết bị điện": "^tbd",
    "Tài chính khác": "^tck",
    "Tiện ích": "^ti",
    "Thực phẩm - Đồ uống": "^tpdu",
    "Vật liệu xây dựng": "^vlxd",
    "Vận tải - kho bãi": "^vtkb",
    "Xây dựng": "^xd",
    "Cao su": "^caosu",
    "Nhóm Dầu khí": "^daukhi",
    "Dược phẩm / Y tế / Hóa chất": "^duocpham",
    "Giáo dục": "^giaoduc",
    "Hàng không": "^hk",
    "Năng lượng (Điện/Khí/...)": "^nangluong",
    "Nhựa - Bao bì": "^nhua",
    "Phân bón": "^phanbon",
    "Ngành Thép": "^thep",
}