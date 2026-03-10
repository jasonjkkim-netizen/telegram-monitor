"""
filters.py - Message filtering functions for telegram-monitor.

Contains crypto keyword filter and earnings/disclosure keyword filter.
"""

from config import (
    CRYPTO_LOWER, ANCHOR_LOWER, DATA_LOWER, HIGH_CONF_LOWER,
    FINANCIAL_NUMBER_PATTERN,
)


def contains_crypto_keyword(text):
    """Check if text contains any cryptocurrency-related keywords."""
    if not text:
        return False
    lower = text.lower()
    return any(kw in lower for kw in CRYPTO_LOWER)


def contains_earnings_keyword(text):
    """Two-layer filter with high-confidence bypass.

    Returns True if:
    - Both an anchor keyword AND a data keyword are present, OR
    - A high-confidence keyword is present AND a financial number pattern is found.
    """
    if not text:
        return False
    lower = text.lower()

    has_anchor = any(kw in lower for kw in ANCHOR_LOWER)
    has_data = any(kw in lower for kw in DATA_LOWER)
    if has_anchor and has_data:
        return True

    has_high_conf = any(kw in lower for kw in HIGH_CONF_LOWER)
    if has_high_conf and FINANCIAL_NUMBER_PATTERN.search(text):
        return True

    return False
