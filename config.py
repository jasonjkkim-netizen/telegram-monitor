"""
config.py - Centralized configuration for telegram-monitor.

All environment variables and shared constants are defined here.
"""

import os
import re

# ============================================================
# TELEGRAM API CONFIGURATION
# ============================================================
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH")
SESSION_STRING = os.environ.get("SESSION_STRING")
TARGET_CHANNEL = os.environ.get("TARGET_CHANNEL")
EARNINGS_CHANNEL = os.environ.get("EARNINGS_CHANNEL")
VOLUME_ALERT_CHANNEL = os.environ.get("VOLUME_ALERT_CHANNEL")

# ============================================================
# THRESHOLDS & TUNING
# ============================================================
SIMILARITY_THRESHOLD = float(os.environ.get("SIMILARITY_THRESHOLD", "0.85"))
VOLUME_THRESHOLD = int(os.environ.get("VOLUME_THRESHOLD", "5000000000"))

# ============================================================
# KIS API CONFIGURATION
# ============================================================
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")
KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"
KIS_KOSPI_MST_URL = "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip"
KIS_KOSDAQ_MST_URL = "https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip"

# Tail lengths per official KIS header structure
KOSPI_TAIL_LEN = 228
KOSDAQ_TAIL_LEN = 222

# ============================================================
# REGEX PATTERNS
# ============================================================
FINANCIAL_NUMBER_PATTERN = re.compile(r'[\d,]+\s*(억|조|원|백만|천만)')
STOCK_CODE_PATTERN = re.compile(r'\b(\d{6})\b')

# ============================================================
# CRYPTO FILTER KEYWORDS (BOT 1) - frozenset for O(1) lookup
# ============================================================
CRYPTO_KEYWORDS = frozenset([
    "bitcoin", "btc", "ethereum", "eth", "crypto", "cryptocurrency",
    "blockchain", "defi", "nft", "altcoin", "altcoins", "binance",
    "coinbase", "upbit", "bithumb", "korbit", "ripple", "xrp",
    "solana", "dogecoin", "doge", "usdt", "usdc", "stablecoin",
    "web3", "airdrop", "staking", "ledger", "metamask", "uniswap",
    "pancakeswap", "cardano", "ada", "polkadot", "dot", "avalanche",
    "avax", "bnb", "tron", "trx", "shiba", "pepe",
    "비트코인", "이더리움", "가상화폐", "암호화폐", "블록체인",
    "디파이", "엔에프티", "알트코인", "업비트", "빗썸", "리플",
    "솔라나", "도지코인", "스테이킹", "채굴", "코인마켓",
    "가상자산", "디지털자산", "바이낸스", "에어드롭",
])

# ============================================================
# 실적 + 공시 ANCHOR KEYWORDS (BOT 2)
# ============================================================
EARNINGS_ANCHOR_KEYWORDS = frozenset([
    "공시", "공시내용", "수시공시", "주요공시", "실적발표", "실적공시",
    "잠정실적", "잠정치", "확정치", "사업보고서", "분기보고서",
    "반기보고서", "연결기준", "별도기준", "실적", "실적시즌",
    "실적쇼크", "실적서프라이즈", "어닝쇼크", "어닝서프라이즈",
])

# ============================================================
# 실적 + 공시 DATA KEYWORDS (BOT 2)
# ============================================================
EARNINGS_DATA_KEYWORDS = frozenset([
    "영업이익", "당기순이익", "순이익", "매출액", "매출", "영업손실",
    "순손실", "당기순손실", "적자전환", "흑자전환", "적자지속",
    "흑자지속", "매출총이익", "EBITDA", "EPS", "BPS", "영업이익률",
    "순이익률", "컨센서스", "전년대비", "전분기대비", "YoY", "QoQ",
    "1분기", "2분기", "3분기", "4분기", "1Q", "2Q", "3Q", "4Q",
    "반기실적", "연간실적", "ROE", "ROA", "PER", "PBR",
    "판매량", "판매실적", "수주", "수주잔고", "수주액", "이익", "순익",
])

# ============================================================
# HIGH-CONFIDENCE EARNINGS KEYWORDS (BOT 2)
# ============================================================
HIGH_CONFIDENCE_EARNINGS_KEYWORDS = frozenset([
    "영업이익", "당기순이익", "순이익", "순익", "매출액",
    "영업손실", "순손실", "당기순손실", "매출",
])

# Pre-compile lowercase sets for fast matching
CRYPTO_LOWER = frozenset(k.lower() for k in CRYPTO_KEYWORDS)
ANCHOR_LOWER = frozenset(k.lower() for k in EARNINGS_ANCHOR_KEYWORDS)
DATA_LOWER = frozenset(k.lower() for k in EARNINGS_DATA_KEYWORDS)
HIGH_CONF_LOWER = frozenset(k.lower() for k in HIGH_CONFIDENCE_EARNINGS_KEYWORDS)
ALL_EARNINGS_LOWER = ANCHOR_LOWER | DATA_LOWER | HIGH_CONF_LOWER
