"""Unit test cho logic backoff của ST01 — pure function, không cần network."""
import random

from platforms.ingestion.base_crawler import RetryConfig, _calc_backoff


def test_backoff_never_exceeds_cap():
    cfg = RetryConfig(base_delay=1.0, cap=10.0, jitter=True)
    for attempt in range(0, 20):
        assert 0 <= _calc_backoff(attempt, cfg) <= cfg.cap


def test_backoff_without_jitter_is_deterministic_exponential():
    cfg = RetryConfig(base_delay=1.0, cap=60.0, jitter=False)
    assert _calc_backoff(0, cfg) == 1.0
    assert _calc_backoff(1, cfg) == 2.0
    assert _calc_backoff(3, cfg) == 8.0
    assert _calc_backoff(10, cfg) == 60.0  # bị chặn bởi cap


def test_full_jitter_is_bounded_by_exponential_window():
    random.seed(42)
    cfg = RetryConfig(base_delay=1.0, cap=60.0, jitter=True)
    samples = [_calc_backoff(2, cfg) for _ in range(200)]
    assert max(samples) <= 4.0          # base * 2^2
    assert len(set(samples)) > 1        # thực sự ngẫu nhiên


def test_default_retryable_codes_include_429():
    assert 429 in RetryConfig().retryable_codes