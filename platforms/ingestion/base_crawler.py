"""
Base HTTP Crawler
=================
Domain-agnostic base class cho web crawlers.

platforms/ layer chỉ định nghĩa:
  - Retry logic
  - Session management
  - Rate limiting
  - Safe text extraction helpers

Domain-specific crawlers (vd: cophieu68, cafef, vnexpress)
được đặt trong flows/<domain>/ingestion/source/
và extend class này.
"""
from __future__ import annotations

import time
import re
import logging
from typing import List, Optional, Union

import requests
from bs4 import BeautifulSoup


class BaseHttpCrawler:
    """
    Base HTTP crawler với retry, rate-limit và session management.
    Không biết gì về domain cụ thể.
    """

    def __init__(
        self,
        base_url: str,
        delay_seconds: float = 0.2,
        timeout_seconds: int = 30,
        headers: Optional[dict] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.delay    = delay_seconds
        self.timeout  = timeout_seconds
        self.logger   = logger or logging.getLogger(__name__)
        self.session  = requests.Session()
        if headers:
            self.session.headers.update(headers)

    def get_soup(self, url: str, retries: int = 3) -> Optional[BeautifulSoup]:
        """Fetch URL với exponential backoff, trả về BeautifulSoup hoặc None."""
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                response.encoding = "utf-8"
                soup = BeautifulSoup(response.text, "html.parser")
                time.sleep(self.delay)
                return soup
            except Exception as exc:
                self.logger.warning(
                    "Fetch %s attempt %d/%d failed: %s", url, attempt + 1, retries, exc
                )
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
        self.logger.error("Failed to fetch %s after %d attempts", url, retries)
        return None

    def safe_extract_text(
        self,
        soup: BeautifulSoup,
        selector: str,
        multiple: bool = False,
    ) -> Union[str, List[str]]:
        """An toàn trích xuất text từ CSS selector."""
        try:
            if multiple:
                return [el.get_text(strip=True) for el in soup.select(selector)]
            el = soup.select_one(selector)
            return el.get_text(strip=True) if el else ""
        except Exception:
            return [] if multiple else ""

    def extract_number(self, text: str) -> str:
        """Lọc ra ký tự số, dấu phẩy, chấm, dấu âm."""
        if not text:
            return ""
        return re.sub(r"[^\d.,\-]", "", text)
