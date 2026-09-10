"""
ExtractCophieu68 — Domain crawler for cophieu68.vn
====================================================
Extends BaseHttpCrawler (platforms/ingestion/base_crawler.py).
SRP: chỉ chứa HTTP fetch + HTML parse logic.
Không chứa: transform, storage, DQ.
"""
from __future__ import annotations

import re
import time
from dataclasses import asdict
from io import StringIO
from typing import Dict, List, Optional

import pandas as pd
from bs4 import BeautifulSoup

from platforms.ingestion.base_crawler import BaseHttpCrawler
from flows.cophieu68_deploy_full_pipeline.ingestion.dto.extract_models import (
    BalanceSheet,
    BusinessPlanRow,
    CompanyBelongToIndustrySector,
    CompanyBelongToMarketType,
    CompanyProfile,
    DetailsMatchReport,
    DetailsMatchRow,
    IncomeStatement,
    IndustryCapitalInfo,
    IndustryFinancialInfo,
    IndustrySummaryInfo,
    StockFinancialRatios,
    StockFinancialReport,
    TradingRecord,
    CRAWL_COMPANY_PROFILE_CONFIG,
    CRAWL_MARKET_LIST_CONFIG,
    INDUSTRIAL_INFO_TYPE,
    MAPPING_INDUSTRY_CODE,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helper — build URL without double-eval (f-string + .format clash)
# ─────────────────────────────────────────────────────────────────────────────

def _build_url(base: str, template: str, **kwargs) -> str:
    """Nối base_url + endpoint template, sau đó substitute {placeholders}."""
    return (base + template).format(**kwargs)


# ─────────────────────────────────────────────────────────────────────────────
# Base layer — HTTP + BeautifulSoup, domain-specific config only
# ─────────────────────────────────────────────────────────────────────────────

class Cophieu68BeautifulSoupCrawler(BaseHttpCrawler):
    """
    Domain-specific crawler cho cophieu68.vn.
    Extends BaseHttpCrawler từ platforms/ingestion/base_crawler.py.
    """

    def __init__(self, pipeline_config: dict, pipeline_logger) -> None:
        cfg = pipeline_config
        self.crawler_cfg = cfg["sources"]["cophieu68"]
        super().__init__(
            base_url=self.crawler_cfg["base_url"],
            delay_seconds=cfg["http"].get("delay_seconds", 0.2),
            timeout_seconds=cfg["http"].get("timeout_seconds", 30),
            headers=cfg["http"].get("headers", {}),
            logger=pipeline_logger,
        )
        # get_soup, safe_extract_text, extract_number — inherited


# ─────────────────────────────────────────────────────────────────────────────
# Concrete extractor — all endpoints
# ─────────────────────────────────────────────────────────────────────────────

class ExtractCophieu68(Cophieu68BeautifulSoupCrawler):
    """
    Concrete crawler cho tất cả endpoints cophieu68.vn.
    """

    def __init__(self, pipeline_config: dict, pipeline_logger) -> None:
        super().__init__(pipeline_config, pipeline_logger)
        self.endpoint: dict = self.crawler_cfg["endpoints"]

    # ------------------------------------------------------------------
    # crawl_financial_report_summary
    # ------------------------------------------------------------------

    def crawl_financial_report_summary(self, symbol: str) -> Optional[Dict]:
        """Crawl trang tóm tắt tài chính — lấy financial_brief + financial_indexes."""
        url = _build_url(self.base_url, self.endpoint["summary_financial"], symbol=symbol.lower())
        soup = self.get_soup(url)
        if not soup:
            self.logger.error("Không thể load trang summary cho %s", symbol)
            return None

        results: Dict = {}

        brief_table = soup.select_one("#financial_brief")
        if brief_table:
            try:
                df = pd.read_html(StringIO(str(brief_table)), flavor="lxml")[0]
                results["financial_brief"] = StockFinancialReport(
                    symbol=symbol.upper(),
                    report_type="brief",
                    table_index=0,
                    data=df,
                )
            except Exception as exc:
                self.logger.warning("Không parse được financial_brief cho %s: %s", symbol, exc)

        indexes_table = soup.select_one("#financial_indexes")
        if indexes_table:
            try:
                df = pd.read_html(StringIO(str(indexes_table)), flavor="lxml")[0]
                results["financial_indexes"] = StockFinancialReport(
                    symbol=symbol.upper(),
                    report_type="indexes",
                    table_index=1,
                    data=df,
                )
            except Exception as exc:
                self.logger.warning("Không parse được financial_indexes cho %s: %s", symbol, exc)

        if not results:
            self.logger.warning("Không tìm thấy bảng nào cho %s", symbol)
            return None
        return results

    # ------------------------------------------------------------------
    # crawl_business_plan
    # ------------------------------------------------------------------

    def crawl_business_plan(self, symbol: str) -> Optional[Dict]:
        """Crawl bảng KẾ HOẠCH KINH DOANH."""
        url = _build_url(self.base_url, self.endpoint["summary_financial"], symbol=symbol.lower())
        soup = self.get_soup(url)
        if not soup:
            return None

        heading = soup.find("h2", string=re.compile(r"KẾ HOẠCH KINH DOANH", re.I))
        if not heading:
            self.logger.info("Không tìm thấy phần KẾ HOẠCH KINH DOANH cho %s", symbol)
            return None

        table = heading.find_next("table")
        if not table:
            self.logger.info("Không tìm thấy bảng kế hoạch kinh doanh cho %s", symbol)
            return None

        def _clean_number(val: str) -> float:
            if not val:
                return 0.0
            val = re.sub(r"\(.*?\)", "", val)
            val = val.replace(",", "").replace("%", "").strip()
            try:
                return float(val)
            except ValueError:
                return 0.0

        rows = []
        for tr in table.select("tr.border_bottom"):
            tds = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(tds) >= 5:
                rows.append(
                    BusinessPlanRow(
                        Year=tds[0],
                        Plan_revenue=_clean_number(tds[1]),
                        Pass_revenue=_clean_number(tds[2]),
                        Plan_profit=_clean_number(tds[3]),
                        Pass_profit=_clean_number(tds[4]),
                    ).__dict__
                )

        if not rows:
            self.logger.info("Không có dữ liệu kế hoạch kinh doanh cho %s", symbol)
            return None
        return {"symbol": symbol.upper(), "data": rows}

    # ------------------------------------------------------------------
    # crawl_details_match
    # ------------------------------------------------------------------

    def crawl_details_match(self, symbol: str) -> Optional[Dict]:
        """Trích xuất phần Chi tiết khớp lệnh."""
        url = _build_url(self.base_url, self.endpoint["summary_financial"], symbol=symbol.lower())
        soup = self.get_soup(url)
        if not soup:
            return None

        heading = soup.find("h2", string=re.compile(r"Chi tiết khớp lệnh", re.I))
        if not heading:
            self.logger.info("Không tìm thấy phần Chi tiết khớp lệnh cho %s", symbol)
            return None

        section = heading.find_next_sibling()
        if not section:
            return None

        rows = []
        for tr in section.select("tr"):
            tds = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(tds) == 5:
                try:
                    rows.append(
                        DetailsMatchRow(
                            Time_match=tds[0],
                            Price_match=float(tds[1].replace(",", "")),
                            Increase_decrease=tds[2],
                            Volume=int(tds[3].replace(",", "")),
                            Accum_volume=int(tds[4].replace(",", "")),
                        ).__dict__
                    )
                except Exception:
                    continue

        if not rows:
            self.logger.info("Không có dòng dữ liệu khớp lệnh cho %s", symbol)
            return None
        return DetailsMatchReport(symbol=symbol.upper(), data=rows).__dict__

    # ------------------------------------------------------------------
    # crawl_detailed_financial_report (internal helper)
    # ------------------------------------------------------------------

    def _crawl_detailed_financial_report(self, symbol: str, report_type: str) -> Optional[Dict]:
        """Internal: crawl tất cả bảng từ trang báo cáo tài chính chi tiết."""
        if report_type == "year":
            url = _build_url(self.base_url, self.endpoint["financial_details_year"], symbol=symbol.lower())
        elif report_type == "quarter":
            url = _build_url(self.base_url, self.endpoint["financial_details_quarter"], symbol=symbol.lower())
        else:
            self.logger.error("Invalid report_type=%s (must be 'quarter' or 'year')", report_type)
            return None

        soup = self.get_soup(url)
        if not soup:
            return None

        results: Dict = {}
        for idx, table in enumerate(soup.find_all("table")):
            try:
                df = pd.read_html(StringIO(str(table)), flavor="lxml")[0]
                results[f"table_{idx}"] = df
            except Exception:
                continue
        return results or None

    def crawl_details_income_statement(self, symbol: str, report_type: str) -> Optional[Dict]:
        """Crawl income statement — table_0 của báo cáo chi tiết."""
        reports = self._crawl_detailed_financial_report(symbol, report_type)
        if not reports or "table_0" not in reports:
            return None
        return IncomeStatement(
            symbol=symbol.upper(),
            report_type=report_type,
            data=reports["table_0"],
        ).__dict__

    def crawl_details_balance_sheet(self, symbol: str, report_type: str) -> Optional[Dict]:
        """Crawl balance sheet — table_1 của báo cáo chi tiết."""
        reports = self._crawl_detailed_financial_report(symbol, report_type)
        if not reports or "table_1" not in reports:
            return None
        return BalanceSheet(
            symbol=symbol.upper(),
            report_type=report_type,
            data=reports["table_1"],
        ).__dict__

    # ------------------------------------------------------------------
    # crawl_company_info_belong_to_industry_sectors
    # ------------------------------------------------------------------

    def crawl_company_info_belong_to_industry_sectors(self) -> List:
        """Crawl danh sách công ty theo ngành."""
        rows = []
        for industry_name, industry_code in MAPPING_INDUSTRY_CODE.items():
            time.sleep(0.25)
            url = _build_url(
                self.base_url,
                self.endpoint["company_industry_sector"],
                industry_code=industry_code,
            )
            try:
                # FIX: không dùng tên biến 're' — shadow module re
                tables = pd.read_html(url)
                data = tables[0].values
            except Exception as exc:
                self.logger.warning("Không đọc được ngành %s: %s", industry_name, exc)
                continue
            for item in data:
                try:
                    parts = str(item[0]).split("  ")
                    if len(parts) < 3:
                        continue
                    rows.append(
                        CompanyBelongToIndustrySector(
                            industry_code=industry_code,
                            industry_name=industry_name,
                            symbol=parts[1].upper(),
                            company_name=parts[2],
                            close_price=float(item[1]) if item[1] else None,
                            Increase_decrease=float(item[2]) if item[2] else None,
                            volumn24h=float(item[3]) if len(item) > 3 and item[3] else None,
                            volumn52w=float(item[4]) if len(item) > 4 and item[4] else None,
                            listed_volumn=float(item[5]) if len(item) > 5 and item[5] else None,
                            market_capitalization=float(item[6]) if len(item) > 6 and item[6] else None,
                        )
                    )
                except Exception:
                    continue
        return rows

    # ------------------------------------------------------------------
    # crawl_company_info_belong_to_market_type
    # ------------------------------------------------------------------

    def crawl_company_info_belong_to_market_type(self) -> List:
        """Crawl danh sách công ty theo sàn."""
        rows = []
        for market_type_name, market_type_code in CRAWL_MARKET_LIST_CONFIG.items():
            time.sleep(0.25)
            url = _build_url(
                self.base_url,
                self.endpoint["company_market_type_sector"],
                market_type_code=market_type_code,
            )
            try:
                # FIX: không dùng tên biến 're' — shadow module re
                tables = pd.read_html(url)
                data = tables[0].values
            except Exception as exc:
                self.logger.warning("Không đọc được sàn %s: %s", market_type_name, exc)
                continue
            for item in data:
                try:
                    parts = str(item[0]).split("  ")
                    if len(parts) < 3:
                        continue
                    rows.append(
                        CompanyBelongToMarketType(
                            market_type_code=market_type_code,
                            market_type_name=market_type_name,
                            symbol=parts[1].upper(),
                            company_name=parts[2],
                            close_price=float(item[1]) if item[1] else None,
                            Increase_decrease=float(item[2]) if item[2] else None,
                            volumn24h=float(item[3]) if len(item) > 3 and item[3] else None,
                            volumn52w=float(item[4]) if len(item) > 4 and item[4] else None,
                            listed_volumn=float(item[5]) if len(item) > 5 and item[5] else None,
                            market_capitalization=float(item[6]) if len(item) > 6 and item[6] else None,
                        )
                    )
                except Exception:
                    continue
        return rows

    # ------------------------------------------------------------------
    # crawl_industry_info
    # ------------------------------------------------------------------

    def crawl_industry_info(self, type_info: str) -> Optional[Dict]:
        """Crawl bảng thông tin ngành (chỉ số TB, EPS, PE, ROA, ROE, ...)."""
        if type_info not in INDUSTRIAL_INFO_TYPE:
            self.logger.error("Invalid industry type: %s", type_info)
            return None

        sub = INDUSTRIAL_INFO_TYPE[type_info]
        if type_info == "summary_info":
            url = f"{self.base_url}{self.endpoint['stock_category'][0]}"
        else:
            url = f"{self.base_url}{self.endpoint['stock_category'][1]}?sub={sub}"

        soup = self.get_soup(url)
        if not soup:
            return None

        rows: Dict = {}
        try:
            table = soup.select_one("table.table_content")
            if not table:
                raise ValueError("Không tìm thấy bảng dữ liệu ngành.")

            for tr in table.select("tr.border_bottom"):
                tds = tr.find_all("td")
                if not tds:
                    continue
                a_tag = tds[0].select_one("a[href]")
                if not a_tag:
                    continue
                code_tag = a_tag.select_one("div:nth-of-type(1)")
                name_tag = a_tag.select_one("div:nth-of-type(2)")
                if not (code_tag and name_tag):
                    continue

                industry_code = code_tag.get_text(strip=True)
                industry_name = name_tag.get_text(strip=True)
                industry_url  = a_tag["href"]

                def _get(idx: int) -> Optional[str]:
                    return tds[idx].get_text(strip=True) if len(tds) > idx else None

                # FIX: row có thể unbound nếu sub không match — dùng None default
                row = None
                if sub == 0:
                    row = IndustrySummaryInfo(
                        index=_get(1), change=_get(2),
                        liquidity=_get(3), capital=_get(4),
                    )
                elif sub == 2:
                    row = IndustryFinancialInfo(
                        avg_price=_get(1), book_value=_get(2),
                        eps=_get(3), pe=_get(4),
                        roa=_get(5), roe=_get(6),
                    )
                elif sub == 3:
                    row = IndustryCapitalInfo(
                        supply_volumn=_get(1), total_asset=_get(2),
                        total_equity=_get(3), total_liabilities=_get(4),
                        percentage_debt_on_equity=_get(5),
                        percentage_equity_on_assets=_get(6),
                        revenue=_get(7), profit_before_tax=_get(8),
                    )

                if row is not None:
                    key = f"_{industry_code}_{industry_name}_{industry_url}_"
                    rows[key] = row.__dict__

        except Exception as exc:
            self.logger.error("Error extracting industry info for %s: %s", type_info, exc)
            return None

        return rows or None

    # ------------------------------------------------------------------
    # crawl_financial_ratios
    # ------------------------------------------------------------------

    def crawl_financial_ratios(
        self,
        symbol: str,
        soup: Optional[BeautifulSoup] = None,
    ) -> Optional[Dict]:
        """Crawl bảng tóm tắt chỉ tiêu tài chính đầu trang cổ phiếu."""
        if not soup:
            # FIX: dùng lower() nhất quán với các endpoint khác
            url = _build_url(self.base_url, self.endpoint["summary_financial"], symbol=symbol.lower())
            soup = self.get_soup(url)

        if not soup:
            return None

        try:
            metrics = StockFinancialRatios(symbol=symbol.upper())
            flex_details = soup.find_all("div", class_="flex_detail")

            if len(flex_details) >= 10:
                # Section 1: Giá + khối lượng
                section1_values = flex_details[1].find_all("div")
                if len(section1_values) >= 5:
                    metrics.reference_price = section1_values[0].get_text(strip=True)
                    metrics.open_price      = section1_values[1].get_text(strip=True)
                    metrics.high_price      = section1_values[2].get_text(strip=True)
                    metrics.low_price       = section1_values[3].get_text(strip=True)
                    metrics.volume          = section1_values[4].get_text(strip=True).replace(",", "")

                # Section 2: Chỉ số tài chính
                section2_values = flex_details[3].find_all("div")
                if len(section2_values) >= 5:
                    metrics.book_value = section2_values[0].get_text(strip=True)
                    metrics.eps        = section2_values[1].get_text(strip=True)
                    metrics.pe         = section2_values[2].get_text(strip=True)
                    metrics.pb         = section2_values[3].get_text(strip=True)
                    # FIX: StockFinancialRatios có roa + roe riêng, không có roa_roe
                    roa_roe_raw = section2_values[4].get_text(strip=True)
                    if "/" in roa_roe_raw:
                        parts = roa_roe_raw.split("/")
                        metrics.roa = parts[0].strip()
                        metrics.roe = parts[1].strip() if len(parts) > 1 else None
                    else:
                        metrics.roa = roa_roe_raw

                # Section 3: Thông tin thị trường
                section3_values = flex_details[5].find_all("div")
                if len(section3_values) >= 5:
                    metrics.beta          = section3_values[0].get_text(strip=True)
                    metrics.market_cap    = section3_values[1].get_text(strip=True)
                    metrics.listed_volume = section3_values[2].get_text(strip=True)
                    metrics.avg_volume_52w = section3_values[3].get_text(strip=True).replace(",", "")
                    metrics.high_low_52w  = section3_values[4].get_text(strip=True)

                # Section 4: Nợ và vốn
                section4_values = flex_details[7].find_all("div")
                if len(section4_values) >= 5:
                    metrics.debt             = section4_values[0].get_text(strip=True)
                    metrics.equity           = section4_values[1].get_text(strip=True)
                    metrics.debt_to_equity   = section4_values[2].get_text(strip=True)
                    metrics.equity_to_assets = section4_values[3].get_text(strip=True)
                    metrics.cash             = section4_values[4].get_text(strip=True)

                # Section 5: Sức mạnh chỉ số
                section5_values = flex_details[9].find_all("div", recursive=False)
                _power_fields = [
                    "eps_power", "roe_power", "invest_efficiency",
                    "pb_power", "price_growth_power",
                ]
                for idx, val_div in enumerate(section5_values[:5]):
                    inner = val_div.find_all("div")
                    if inner:
                        setattr(metrics, _power_fields[idx], inner[-1].get_text(strip=True))

            return metrics.__dict__

        except Exception as exc:
            self.logger.error("Error extracting financial ratios for %s: %s", symbol, exc)
            return None

    # ------------------------------------------------------------------
    # crawl_trading_data
    # ------------------------------------------------------------------

    def crawl_trading_data(self, symbol: str, page: Optional[int] = None) -> Optional[Dict]:
        """Crawl lịch sử giao dịch, trả về dict {symbol, records}."""
        all_rows = []
        page_num = 1 if page is None else page

        while True:
            url = _build_url(
                self.base_url,
                self.endpoint["trading_data"],
                page=page_num,
                symbol=symbol.upper(),
            )
            soup = self.get_soup(url)
            if soup is None:
                self.logger.warning("Không lấy được dữ liệu trang %d cho %s", page_num, symbol)
                break

            table = soup.find("table", {"id": "history"})
            if not table:
                self.logger.warning("Không tìm thấy bảng lịch sử trang %d", page_num)
                break

            page_rows = []
            for tr in table.find_all("tr")[1:]:
                tds = [
                    td.get_text(strip=True).replace(",", "").replace("\xa0", "")
                    for td in tr.find_all("td")
                ]
                if len(tds) == 9:
                    page_rows.append(tds)

            if not page_rows:
                self.logger.info("Hết dữ liệu ở trang %d", page_num)
                break

            all_rows.extend(page_rows)
            if page is not None:
                break   # single-page mode

            page_num += 1
            time.sleep(self.delay)

        if not all_rows:
            return None

        records = []
        for row in all_rows:
            try:
                records.append(
                    TradingRecord(
                        date=row[0],
                        close_price=float(row[1]),
                        volume=int(float(row[2])),
                        open_price=float(row[3]),
                        high_price=float(row[4]),
                        low_price=float(row[5]),
                        foreign_buy=int(float(row[6])),
                        foreign_sell=int(float(row[7])),
                        foreign_value=float(row[8]),
                    )
                )
            except Exception as exc:
                self.logger.warning("Lỗi parse dòng dữ liệu %s: %s", row, exc)

        return {"symbol": symbol.upper(), "records": [asdict(r) for r in records]}

    # ------------------------------------------------------------------
    # crawl_company_profile
    # ------------------------------------------------------------------

    def crawl_company_profile(self, symbol: str) -> Optional[CompanyProfile]:
        """Crawl thông tin chi tiết công ty từ trang profile."""
        url = _build_url(
            self.base_url, self.endpoint["company_profile"], symbol=symbol.upper()
        )
        soup = self.get_soup(url)
        if not soup:
            return None

        profile = CompanyProfile(symbol=symbol.upper())
        field_map = CRAWL_COMPANY_PROFILE_CONFIG["field_map"]

        profile_table = None
        for tbl in soup.find_all("table"):
            if tbl.find("td", string=lambda x: x and "Mã CK" in x):
                profile_table = tbl
                break

        if not profile_table:
            return profile

        def _clean(text: str) -> str:
            return re.sub(r"\s+", " ", text).strip()

        for row in profile_table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            label = _clean(cells[0].get_text()).lower()
            value = _clean(cells[1].get_text())
            for key, attr in field_map.items():
                if key.lower() in label:
                    if not getattr(profile, attr, None):
                        setattr(profile, attr, value)
                    break

        return profile

    # ------------------------------------------------------------------
    # crawl_market_list
    # ------------------------------------------------------------------

    def crawl_market_list(self, market_type: str) -> Dict:
        """Crawl danh sách mã cổ phiếu theo loại thị trường."""
        if market_type not in CRAWL_MARKET_LIST_CONFIG:
            self.logger.error("Invalid market type: %s", market_type)
            return {}

        url = f"{self.base_url}{self.endpoint['market_list']}?id=^{market_type}"
        soup = self.get_soup(url)
        if not soup:
            return {}

        symbols = []
        for tr in soup.find_all("tr", class_="stock_online"):
            code = tr.get("data-id")
            if code:
                symbols.append(code.upper())

        return {"market_type": market_type, "symbols": symbols}
