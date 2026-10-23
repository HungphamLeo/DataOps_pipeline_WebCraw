"""
platforms/ingestion/base_crawler.py
=====================================
BaseHttpCrawler — HTTP client chuẩn cho mọi web crawler trong hệ thống.

Chức năng:
  - HTTP GET với Exponential Backoff + Full Jitter (chống Thundering Herd)
  - Xử lý HTTP 429: đọc Retry-After header, override delay nếu lớn hơn
  - Mỗi crawl session có correlation_id riêng để trace log end-to-end
  - Trả về None khi fail (không raise) — caller tự xử lý None check
  - Structured JSON log mỗi attempt: attempt_number, wait_seconds, status_code

SRP: chỉ chứa HTTP transport + retry logic.
Không chứa: HTML parse, business logic, storage, DQ.
"""
from __future__ import annotations

import random
import time
import uuid
from typing import Any, Dict, Optional
from dataclasses import dataclass, field

import requests
from bs4 import BeautifulSoup

from shared.logger.logger_setup import logger_manager


@dataclass
class RetryConfig:
    """Cấu hình retry cho HTTP requests.

    Attributes:
        max_attempts:   Tổng số lần thử (lần đầu + các lần retry).
        base_delay:     Delay cơ sở (giây) cho lần retry đầu tiên.
        cap:            Delay tối đa (giây) — tránh backoff vô hạn.
        jitter:         Bật Full Jitter để tránh Thundering Herd.
        retryable_codes: HTTP status codes kích hoạt retry.
    """
    max_attempts:    int        = 5
    base_delay:      float      = 1.0
    cap:             float      = 60.0
    jitter:          bool       = True
    retryable_codes: list       = field(default_factory=lambda: [429, 500, 502, 503, 504])


def _calc_backoff(attempt: int, cfg: RetryConfig) -> float:
    """Tính thời gian chờ theo Exponential Backoff + Full Jitter.

    Công thức Full Jitter (AWS): sleep = random(0, min(cap, base * 2^attempt))
    Đây là cách tốt nhất để tránh Thundering Herd khi nhiều worker
    cùng retry một lúc — mỗi worker chờ một khoảng thời gian ngẫu nhiên khác nhau.
    """
    exponential = min(cfg.cap, cfg.base_delay * (2 ** attempt))
    if cfg.jitter:
        return random.uniform(0, exponential)
    return exponential


class BaseHttpCrawler:
    """
    HTTP client nền tảng cho tất cả web crawlers.

    Subclass kế thừa class này và gọi:
        - self.get_soup(url)    → Optional[BeautifulSoup]
        - self.base_url         → str
        - self.delay            → float
        - self.logger           → Logger

    Không cần override gì — chỉ cần super().__init__(...) với đúng params.
    """

    def __init__(
        self,
        base_url:        str,
        delay_seconds:   float = 0.2,
        timeout_seconds: int   = 30,
        headers:         Optional[Dict[str, str]] = None,
        retry_config:    Optional[RetryConfig]    = None,
        logger=None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.delay    = delay_seconds
        self.timeout  = timeout_seconds
        self.headers  = headers or {}
        self.retry    = retry_config or RetryConfig()
        self.logger   = logger or logger_manager.get_logger(__name__)

        # Session tái sử dụng TCP connection — giảm overhead mỗi request
        self._session = requests.Session()
        self._session.headers.update(self.headers)

    # ------------------------------------------------------------------
    # Public API — dùng trong subclass
    # ------------------------------------------------------------------

    def get_soup(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch URL với retry, parse HTML → BeautifulSoup.

        Trả về None nếu tất cả attempts đều fail.
        Caller phải check: if not soup: return None
        """
        correlation_id = str(uuid.uuid4())
        response = self._http_get_with_retry(url, correlation_id)
        if response is None:
            return None
        try:
            return BeautifulSoup(response.text, "lxml")
        except Exception as exc:
            self.logger.error(
                "[Crawler] HTML parse failed url=%s error=%s correlation_id=%s",
                url, exc, correlation_id,
            )
            return None

    # ------------------------------------------------------------------
    # Internal — retry engine
    # ------------------------------------------------------------------

    def _http_get_with_retry(
        self,
        url: str,
        correlation_id: str,
    ) -> Optional[requests.Response]:
        """Thực hiện HTTP GET với Exponential Backoff + Full Jitter.

        Logic:
            1. Gửi request
            2a. 200 → trả về Response ngay
            2b. 429 → đọc Retry-After header, chờ max(calculated, retry_after)
            2c. 5xx → chờ theo backoff formula
            2d. Lỗi network → chờ theo backoff formula
            3. Hết max_attempts → trả về None
        """
        for attempt in range(self.retry.max_attempts):
            try:
                response = self._session.get(url, timeout=self.timeout)

                if response.status_code == 200:
                    self.logger.debug(
                        "[Crawler] OK attempt=%d url=%s correlation_id=%s",
                        attempt + 1, url, correlation_id,
                    )
                    return response

                if response.status_code not in self.retry.retryable_codes:
                    # 4xx không retry được (trừ 429) — fail ngay
                    self.logger.warning(
                        "[Crawler] Non-retryable status=%d url=%s correlation_id=%s",
                        response.status_code, url, correlation_id,
                    )
                    return None

                # Tính thời gian chờ
                wait = _calc_backoff(attempt, self.retry)

                # 429: đọc Retry-After header, dùng nếu lớn hơn backoff
                if response.status_code == 429:
                    retry_after = self._parse_retry_after(response)
                    if retry_after is not None:
                        wait = max(wait, retry_after)

                self.logger.warning(
                    "[Crawler] Retryable status=%d attempt=%d/%d wait_seconds=%.1f "
                    "url=%s correlation_id=%s",
                    response.status_code,
                    attempt + 1,
                    self.retry.max_attempts,
                    wait,
                    url,
                    correlation_id,
                )
                time.sleep(wait)

            except requests.exceptions.Timeout:
                wait = _calc_backoff(attempt, self.retry)
                self.logger.warning(
                    "[Crawler] Timeout attempt=%d/%d wait_seconds=%.1f "
                    "url=%s correlation_id=%s",
                    attempt + 1, self.retry.max_attempts, wait, url, correlation_id,
                )
                time.sleep(wait)

            except requests.exceptions.ConnectionError as exc:
                wait = _calc_backoff(attempt, self.retry)
                self.logger.warning(
                    "[Crawler] ConnectionError=%s attempt=%d/%d wait_seconds=%.1f "
                    "url=%s correlation_id=%s",
                    exc, attempt + 1, self.retry.max_attempts, wait, url, correlation_id,
                )
                time.sleep(wait)

            except requests.exceptions.RequestException as exc:
                # Lỗi không retry được (SSL, invalid URL, ...)
                self.logger.error(
                    "[Crawler] Fatal request error=%s url=%s correlation_id=%s",
                    exc, url, correlation_id,
                )
                return None

        self.logger.error(
            "[Crawler] All %d attempts failed url=%s correlation_id=%s",
            self.retry.max_attempts, url, correlation_id,
        )
        return None

    @staticmethod
    def _parse_retry_after(response: requests.Response) -> Optional[float]:
        """Đọc Retry-After header, trả về số giây cần chờ.

        Header có 2 dạng: số giây (vd "30") hoặc HTTP date (vd "Fri, 31 Dec ...").
        Chỉ handle dạng số giây — đủ cho hầu hết SaaS/web target.
        """
        header = response.headers.get("Retry-After")
        if header is None:
            return None
        try:
            return float(header)
        except (ValueError, TypeError):
            return None