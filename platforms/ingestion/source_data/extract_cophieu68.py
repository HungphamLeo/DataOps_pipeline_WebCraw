from bs4 import BeautifulSoup
import requests
import re
import time
from typing import Optional, List, Union, Dict
import pandas as pd
from io import StringIO
from dataclasses import asdict
from  platforms.ingestion.cophieu68.dto.extract_models import *


class Cophieu68BeautifulSoupCrawler:
    def __init__(self, pipeline_config, pipeline_logger):
        config = pipeline_config
        self.crawler_cfg = config["sources"]["cophieu68"]
        self.urls = self.crawler_cfg["base_url"]
        self.delay = config["http"].get("delay_seconds", 0.2)
        self.timeout =  config["http"].get("timeout_seconds", 30)
        self.session = requests.Session()
        self.session.headers.update(config["http"].get("headers", {}))
        self.logger = pipeline_logger


    def get_soup(self, url: str, retries: int = 3) -> Optional[BeautifulSoup]:
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                response.encoding = "utf-8"
                soup = BeautifulSoup(response.text, "html.parser")
                time.sleep(self.delay)
                return soup
            except Exception as e:
                self.logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    self.logger.error(f"Failed to fetch {url} after {retries} attempts")
                    return None

    def safe_extract_text(self, soup: BeautifulSoup, 
                                selector: str, 
                                multiple: bool = False) -> Union[str, List[str]]:
        """
        An toàn trích xuất text từ selector

        :param soup: Điểm khởi đầu để tìm kiếm
        :param selector: Chọn lọc để tìm kiếm
        :param multiple: Nếu True, trả về List[str], ngược lại trả về str
        :return: Text được trích xuất nếu thành công, ngược lại trả về rỗng
        """
        try:
            if multiple:
                return [el.get_text(strip=True) for el in soup.select(selector)]
            element = soup.select_one(selector)
            return element.get_text(strip=True) if element else ""
        except Exception:
            return [] if multiple else ""

    def extract_number(self, text: str) -> str:
        if not text:
            return ""
        return re.sub(r"[^\d.,\-]", "", text)

class ExtractCophieu68(Cophieu68BeautifulSoupCrawler):
    def __init__(self, pipeline_config=None, pipeline_logger=None):
        if pipeline_config is not None or pipeline_logger is not None:
            super().__init__(pipeline_config, pipeline_logger)
        else:
            super().__init__()
        self.endpoint = self.crawler_cfg["endpoints"]
    
    
    def crawl_financial_report_summary(self, symbol: str) -> Optional[Dict]:
        """Crawl financial report from summary page"""
        url = f"{self.urls}{self.endpoint['summary_financial']}".format(symbol=symbol.lower())
        
        try:
            soup = self.get_soup(url)
            if not soup:
                self.logger.error(f"Không thể load trang summary cho {symbol}")
                return None

            results = {}
            
            # Lấy bảng tóm tắt báo cáo tài chính (financial_brief)
            brief_table = soup.select_one("#financial_brief")
            if brief_table:
                try:
                    brief_df = pd.read_html(StringIO(str(brief_table)), flavor="lxml")[0]
                    results["financial_brief"] = StockFinancialReport(
                        symbol=symbol.upper(), 
                        report_type="brief",
                        table_index=0,
                        data=brief_df
                    )
                    
                except Exception as e:
                    self.logger.warning(f"Không parse được financial_brief: {e}")

            # Lấy bảng chỉ số tăng trưởng tài chính (financial_indexes)
            indexes_table = soup.select_one("#financial_indexes")
            if indexes_table:
                try:
                    indexes_df = pd.read_html(StringIO(str(indexes_table)), flavor="lxml")[0]
                    results["financial_indexes"] = StockFinancialReport(
                        symbol=symbol.upper(),
                        report_type="indexes", 
                        table_index=1,
                        data=indexes_df
                    )
                    
                except Exception as e:
                    self.logger.warning(f"Không parse được financial_indexes: {e}")

            if not results:
                self.logger.warning(f"Không tìm thấy bảng nào cho {symbol}")
                return None
                
            return results

        except Exception as e:
            self.logger.error(f"Error fetching report for {symbol.upper()}: {e}")
            return None
        
    def crawl_business_plan(self, symbol: str):
        """Crawl bảng KẾ HOẠCH KINH DOANH"""
        url = f"{self.urls}{self.endpoint['summary_financial']}".format(symbol=symbol.lower())
        soup = self.get_soup(url)
        if not soup:
            return None

        heading = soup.find("h2", string=re.compile("KẾ HOẠCH KINH DOANH", re.I))
        if not heading:
            self.logger.info(f"Không tìm thấy phần KẾ HOẠCH KINH DOANH cho {symbol}")
            return None

        # Lấy bảng kế tiếp
        table = heading.find_next("table")
        if not table:
            self.logger.info(f"Không tìm thấy bảng kế hoạch kinh doanh cho {symbol}")
            return None

        def clean_number(val: str) -> float:
            """Chuẩn hóa chuỗi số — chỉ lấy phần trước dấu ( nếu có)"""
            if not val:
                return 0.0
            val = re.sub(r"\(.*?\)", "", val)  # bỏ phần trong ngoặc
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
                        Plan_revenue=clean_number(tds[1]),
                        Pass_revenue=clean_number(tds[2]),
                        Plan_profit=clean_number(tds[3]),
                        Pass_profit=clean_number(tds[4]),
                    ).__dict__
                )

        if not rows:
            self.logger.info(f"Không có dữ liệu kế hoạch kinh doanh cho {symbol}")
            return None

        return {"symbol": symbol.upper(), "data": rows}


    def crawl_details_match(self, symbol: str) -> Optional[DetailsMatchReport]:
        """Trích xuất phần 'Chi tiết khớp lệnh'"""
        url = f"{self.urls}{self.endpoint['summary_financial']}".format(symbol=symbol.lower())
        soup = self.get_soup(url)
        if not soup:
            return None

        heading = soup.find("h2", string=re.compile("Chi tiết khớp lệnh", re.I))
        if not heading:
            self.logger.info(f"Không tìm thấy phần Chi tiết khớp lệnh cho {symbol}")
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
            self.logger.info(f"Không có dòng dữ liệu khớp lệnh cho {symbol}")
            return None


        return DetailsMatchReport(symbol=symbol.upper(), data=rows).__dict__
        

    def crawl_detailed_financial_report(self, symbol: str, report_type: str = "quarter") -> Optional[Dict]:
        """
        Crawl detailed financial reports
        report_type: 'quarter' hoặc 'year'
        """
        # URL cho báo cáo chi tiết
        if report_type not in ["quarter", "year"]:
            self.logger.error(f"Invalid report_type: {report_type}, must be 'quarter' or 'year'")
            return None
        elif report_type == "year":
            url = f"{self.urls}{self.endpoint['financial_details_year']}".format(symbol=symbol.lower())
        else:
            url = f"{self.urls}{self.endpoint['financial_details_quarter']}".format(symbol=symbol.lower())
       
        try:
            soup = self.get_soup(url)
            if not soup:
                return None

            results = {}
            
            # Tìm các bảng theo class hoặc cấu trúc HTML thực tế
            # Cần kiểm tra HTML thực tế của trang này để biết selector chính xác
            tables = soup.find_all("table")
            
            for idx, table in enumerate(tables):
                try:
                    df = pd.read_html(StringIO(str(table)), flavor="lxml")[0]
                    results[f"table_{idx}"] = df
                except Exception as e:
                    continue
                    
            return results
            
        except Exception as e:
            self.logger.error(f"Error fetching detailed report: {e}")
            return None
    
    
    def crawl_details_income_statement(self, symbol: str, report_type: str) -> Optional[IncomeStatement]:
        """
        Crawl detailed income statement data
        Parameters:
            symbol (str): Symbol of the stock
            report_type (str): Type of the report (quarter or year)
        Returns:
            Optional[IncomeStatement]: Detailed income statement data
        """
        try:
            reports = self.crawl_detailed_financial_report(symbol, report_type)
            income_report = reports["table_0"] if reports else None
            return IncomeStatement(symbol=symbol.upper(), report_type=report_type, data=income_report).__dict__
        except Exception as e:
            self.logger.error(f"Error crawling income statement for {symbol}: {e}")
            return None


    def crawl_details_balance_sheet(self, symbol: str, report_type: str) -> Optional[BalanceSheet]:
        """
        Crawl detailed balance sheet data
        Parameters:
            symbol (str): Symbol of the stock
            report_type (str): Type of the report (quarter or year)
        Returns:
            Optional[BalanceSheet]: Detailed balance sheet data
        """
        try:
            reports = self.crawl_detailed_financial_report(symbol, report_type)
            balance_report = reports["table_1"] if reports else None
            
            return BalanceSheet(symbol=symbol.upper(), report_type=report_type, data=balance_report).__dict__
        except Exception as e:
            self.logger.error(f"Error crawling balance sheet for {symbol}: {e}")
            return None


    def crawl_company_info_belong_to_industry_sectors(self):
        """Crawl danh sách công ty theo ngành"""
        rows = []
        for industry_name, industry_code in MAPPING_INDUSTRY_CODE.items():
            time.sleep(0.25)
            url = f"{self.urls}{self.endpoint['company_industry_sector']}".format(industry_code=industry_code)
            re = pd.read_html(url)
            data = re[0].values
            for item in data:
                try:
                    rows.append(
                        CompanyBelongToIndustrySector(
                        industry_code=industry_code,
                        industry_name=industry_name,
                        symbol=item[0].split("  ")[1].upper(),
                        company_name=item[0].split("  ")[2],
                        close_price=float(item[1]),
                        Increase_decrease=float(item[2]),
                        volumn24h=float(item[3]) if item[3] else None,
                        volumn52w=float(item[4]) if item[4] else None,
                        listed_volumn=float(item[5]) if item[5] else None,
                        market_capitalization=float(item[6]) if item[6] else None
                        )
                    )
                except Exception:
                    continue
        return rows

    def crawl_company_info_belong_to_market_type(self):
        """Crawl danh sách công ty theo sàn"""
        rows = []
        for market_type_name, market_type_code in CRAWL_MARKET_LIST_CONFIG.items():
            time.sleep(0.25)
            url = f"{self.urls}{self.endpoint['company_market_type_sector']}".format(market_type_code=market_type_code)
            re = pd.read_html(url)
            data = re[0].values
            for item in data:
                try:
                    rows.append(
                        CompanyBelongToMarketType(
                        market_type_code=market_type_code,
                        market_type_name=market_type_name,
                        symbol=item[0].split("  ")[1].upper(),
                        company_name=item[0].split("  ")[2],
                        close_price=float(item[1]),
                        Increase_decrease=float(item[2]),
                        volumn24h=float(item[3]) if item[3] else None,
                        volumn52w=float(item[4]) if item[4] else None,
                        listed_volumn=float(item[5]) if item[5] else None,
                        market_capitalization=float(item[6]) if item[6] else None
                        )
                    )
                except Exception:
                    continue
        return rows

    def crawl_industry_info(self, type_info: str) -> Optional[pd.DataFrame]:
        """Crawl bảng thông tin ngành (Giá TB, Giá sổ sách, EPS, PE, ROA, ROE)"""
        if type_info not in INDUSTRIAL_INFO_TYPE:
            self.logger.error(f"Invalid industry type: {type_info}")
            return None

        url = None
        if type_info == "summary_info":
            url = f"{self.urls}{self.endpoint['stock_category'][0]}"
        else:
            sub = INDUSTRIAL_INFO_TYPE[type_info]
            url = f"{self.urls}{self.endpoint['stock_category'][1]}?sub={sub}"
        # self.logger.info(f"Crawling industry info for type {type_info} from {url}")
        soup = self.get_soup(url)
        rows = {}
        if not soup:
            return None

        try:
            table = soup.select_one("table.table_content")
            if not table:
                raise ValueError("Không tìm thấy bảng dữ liệu ngành trong HTML.")

            sub = INDUSTRIAL_INFO_TYPE[type_info]

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
                industry_url = a_tag["href"]

                def get_text_safe(idx):
                    return tds[idx].get_text(strip=True) if len(tds) > idx else None

                # match-case: loại bảng
                match sub:
                    case 0:
                        row = IndustrySummaryInfo(
                            index=get_text_safe(1),
                            change=get_text_safe(2),
                            liquidity=get_text_safe(3),
                            capital=get_text_safe(4),
                        )
                    case 2:
                        row = IndustryFinancialInfo(
                            avg_price=get_text_safe(1),
                            book_value=get_text_safe(2),
                            eps=get_text_safe(3),
                            pe=get_text_safe(4),
                            roa=get_text_safe(5),
                            roe=get_text_safe(6),
                        )
                    case 3:
                        row = IndustryCapitalInfo(
                            supply_volumn = get_text_safe(1),
                            total_asset=get_text_safe(2),
                            total_equity=get_text_safe(3),
                            total_liabilities=get_text_safe(4),
                            percentage_debt_on_equity=get_text_safe(5),
                            percentage_equity_on_assets=get_text_safe(6),
                            revenue=get_text_safe(7),
                            profit_before_tax=get_text_safe(8),
                        )
                if row:
                    key_industry = f"_{industry_code}_{industry_name}_{industry_url}_"
                    rows[key_industry] = row.__dict__
                    
            return rows

        except Exception as e:
            self.logger.error(f"Error extracting industry info for type {type_info}: {e}")
            return None


    
    def crawl_financial_ratios(self, symbol: str, soup: BeautifulSoup = None) -> Optional[StockFinancialRatios]:
        """Crawl bảng tóm tắt các chỉ tiêu tài chính trên trang chi tiết cổ phiếu"""
        
        if not soup:
            url = f"{self.urls}{self.endpoint['summary_financial']}".format(symbol=symbol.upper())
            soup = self.get_soup(url)

        try:
            metrics = StockFinancialRatios(symbol=symbol.upper())
            
            # Tìm tất cả các div có class flex_detail
            flex_details = soup.find_all('div', class_='flex_detail')
            
            if len(flex_details) >= 10:  # Đảm bảo có đủ sections
                # Section 1: Giá và khối lượng (2 divs đầu trong flex_row đầu tiên)
                section1_labels = flex_details[0].find_all('div')
                section1_values = flex_details[1].find_all('div')
                
                if len(section1_values) >= 5:
                    metrics.reference_price = section1_values[0].get_text(strip=True)
                    metrics.open_price = section1_values[1].get_text(strip=True)
                    metrics.high_price = section1_values[2].get_text(strip=True)
                    metrics.low_price = section1_values[3].get_text(strip=True)
                    metrics.volume = section1_values[4].get_text(strip=True).replace(',', '')
                
                # Section 2: Các chỉ số tài chính
                section2_values = flex_details[3].find_all('div')
                
                if len(section2_values) >= 5:
                    metrics.book_value = section2_values[0].get_text(strip=True)
                    metrics.eps = section2_values[1].get_text(strip=True)
                    metrics.pe = section2_values[2].get_text(strip=True)
                    metrics.pb = section2_values[3].get_text(strip=True)
                    metrics.roa_roe = section2_values[4].get_text(strip=True)
                
                # Section 3: Thông tin thị trường
                section3_values = flex_details[5].find_all('div')
                
                if len(section3_values) >= 5:
                    metrics.beta = section3_values[0].get_text(strip=True)
                    metrics.market_cap = section3_values[1].get_text(strip=True)
                    metrics.listed_volume = section3_values[2].get_text(strip=True)
                    metrics.avg_volume_52w = section3_values[3].get_text(strip=True).replace(',', '')
                    metrics.high_low_52w = section3_values[4].get_text(strip=True)
                
                # Section 4: Nợ và vốn
                section4_values = flex_details[7].find_all('div')
                
                if len(section4_values) >= 5:
                    metrics.debt = section4_values[0].get_text(strip=True)
                    metrics.equity = section4_values[1].get_text(strip=True)
                    metrics.debt_to_equity = section4_values[2].get_text(strip=True)
                    metrics.equity_to_assets = section4_values[3].get_text(strip=True)
                    metrics.cash = section4_values[4].get_text(strip=True)
                
                # Section 5: Sức mạnh chỉ số (cần parse từ progress bar)
                section5_values = flex_details[9].find_all('div', recursive=False)
                
                for idx, val_div in enumerate(section5_values):
                    if idx >= 5:
                        break
                        
                    # Tìm text trong div cuối cùng (chứa phần trăm hoặc rating)
                    inner_divs = val_div.find_all('div')
                    if inner_divs:
                        value = inner_divs[-1].get_text(strip=True)
                        
                        if idx == 0:
                            metrics.eps_power = value
                        elif idx == 1:
                            metrics.roe_power = value
                        elif idx == 2:
                            metrics.invest_efficiency = value
                        elif idx == 3:
                            metrics.pb_power = value
                        elif idx == 4:
                            metrics.price_growth_power = value

            return metrics.__dict__

        except Exception as e:
            self.logger.error(f"Error extracting financial ratios for {symbol}: {e}")
            import traceback
            traceback.print_exc()
            return None
        
    def crawl_trading_data(self, symbol: str, page: Optional[int] = None) -> Optional[str]:
        """Crawl dữ liệu lịch sử giao dịch cổ phiếu và trả về JSON"""
        all_rows = []
        page_num = 1 if page is None else page

        while True:
            try:
                url = f"{self.urls}{self.endpoint['trading_data']}".format(page=page_num, symbol=symbol.upper())
                soup = self.get_soup(url)
                if soup is None:
                    self.logger.warning(f"Không lấy được dữ liệu trang {page_num} cho {symbol}")
                    break

                table = soup.find("table", {"id": "history"})
                if not table:
                    self.logger.warning(f"Không tìm thấy bảng lịch sử giao dịch trên trang {page_num}")
                    break

                rows = []
                for tr in table.find_all("tr")[1:]:
                    tds = [td.get_text(strip=True).replace(",", "").replace("\xa0", "") for td in tr.find_all("td")]
                    if len(tds) == 9:
                        rows.append(tds)

                if not rows:
                    self.logger.info(f"Hết dữ liệu ở trang {page_num}")
                    break

                all_rows.extend(rows)
                if page is not None:
                    break

                page_num += 1
                time.sleep(self.delay)

            except Exception as e:
                self.logger.error(f"Lỗi khi crawl dữ liệu {symbol} trang {page_num}: {e}")
                break

        if not all_rows:
            return None

        records = []
        for r in all_rows:
            try:
                record = TradingRecord(
                    date=r[0],
                    close_price=float(r[1]),
                    volume=int(float(r[2])),
                    open_price=float(r[3]),
                    high_price=float(r[4]),
                    low_price=float(r[5]),
                    foreign_buy=int(float(r[6])),
                    foreign_sell=int(float(r[7])),
                    foreign_value=float(r[8])
                )
                records.append(record)
            except Exception as e:
                self.logger.warning(f"Lỗi parse dòng dữ liệu {r}: {e}")

        return {
                    "symbol": symbol.upper(),
                    "records": [asdict(r) for r in records]
                }

    
    def crawl_company_profile(self, symbol: str) -> Optional[CompanyProfile]:
        """Crawl thông tin chi tiết công ty từ trang profile"""

        url = f"{self.urls}{self.endpoint['company_profile']}".format(
            symbol=symbol.upper()
        )
        soup = self.get_soup(url)
        if not soup:
            return None

        # Khởi tạo object kết quả
        profile = CompanyProfile(symbol=symbol.upper())

        field_map = CRAWL_COMPANY_PROFILE_CONFIG["field_map"]

        # ===== 1. Chỉ tìm bảng thông tin công ty (bảng có 'Mã CK') =====
        profile_table = None
        for table in soup.find_all("table"):
            if table.find("td", string=lambda x: x and "Mã CK" in x):
                profile_table = table
                break

        if not profile_table:
            return profile

        # ===== 2. Hàm làm sạch text =====
        def clean_text(text: str) -> str:
            text = re.sub(r"\s+", " ", text)
            return text.strip()

        # ===== 3. Duyệt từng dòng trong bảng =====
        for row in profile_table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue

            label = clean_text(cells[0].get_text()).lower()
            value = clean_text(cells[1].get_text())

            # ===== 4. Map label → field =====
            for key, attr in field_map.items():
                if key.lower() in label:
                    # Không cho ghi đè dữ liệu đã có
                    if getattr(profile, attr):
                        break
                    setattr(profile, attr, value)
                    break
        return profile

        
    def crawl_market_list(self, market_type: str) -> List[str]:
        """
        Crawl danh sách mã cổ phiếu từ thị trường
        
        :param market_type: Loại thị trường (VN_INDEX, HOSE, HNX, UPCOM)
        :return: Danh sách mã cổ phiếu
        """
        if market_type not in CRAWL_MARKET_LIST_CONFIG:
            self.logger.error(f"Invalid market type: {market_type}")
            return []
        url = f"{self.urls}{self.endpoint['market_list']}?id=^{market_type}"
        soup = self.get_soup(url)
        if not soup:
            return []

        try:
            symbols = []
            # Find all table rows with class "stock_online"
            for tr in soup.find_all("tr", class_="stock_online"):
                # Get the stock code from the "data-id" attribute
                code = tr.get("data-id")
                if code:
                    symbols.append(code.upper())
            return {"market_type": market_type, "symbols": symbols}

        except Exception as e:
            self.logger.error(f"Error extracting market list: {e}")
            return []



    
   