"""
Telegram Channel & Group Monitor v7.5
======================================
BOT 1: All unique messages (no crypto/coin) -> TARGET_CHANNEL (@my_filtered_news)
BOT 2: 실적/공시 keyword messages -> EARNINGS_CHANNEL (@jason_earnings)
BOT 3: 종목 언급 시 -> 현재가, 거래대금, RISK, 이동평균 상태 알림 -> VOLUME_ALERT_CHANNEL (@alerts_forme)

v7.5 Changes:
    - BOT 2: Added standalone keywords "이익" and "순익" to EARNINGS_DATA_KEYWORDS
    - BOT 2: Added high-confidence keyword bypass (영업이익, 당기순이익, 순이익, 순익, 매출액
      with numbers can trigger without anchor keyword)
    - BOT 2: Now also checks OCR text from images for earnings keywords
    - Reduces missed financial reports from image-based messages

v7.3 Changes:
    - BOT 2: Split EARNINGS_KEYWORDS into anchor + data two-layer filter
    - Requires both a 공시/실적 context keyword AND a financial data keyword
    - Reduces false positives from general market commentary

v7.2 Changes:
    - Fixed BOT 3: stock name now reliably shown in alerts
    - Added stock_universe.lookup_name() fallback when KIS API returns code instead of name
    - Name resolution priority: KIS API name > stock universe name > stock code

v7.1 Changes:
    - Added OCR: extracts stock names from image messages (Tesseract + Korean)
    - Replaced hardcoded STOCK_MAP with dynamic universe from KIS master files
    - Downloads kospi_code.mst.zip + kosdaq_code.mst.zip at startup
    - Refreshes stock universe every 24 hours
    - BOT 3 now processes both text AND image messages for stock detection

v7.0 Changes:
    - Redesigned BOT 3: now alerts on EVERY stock mention (not just volume threshold)
    - BOT 3 now shows: price, change rate, aggregated volume,
      RISK level (based on price change %), and moving average state
    - Added get_daily_close_prices() for MA calculation (5/20/60 day)
    - BOT 3 runs independently from BOT 2 (not nested inside earnings block)

v6.1 Changes:
    - Changed BOT 3 volume check from cumulative daily (acml_tr_pbmn) to
      rolling 20-minute window using minute-candle API (FHKST03010200)

v6.0 Fixes:
    - Fixed aiohttp session: removed base_url, using full URLs
    - Fixed BOT 3: only runs inside BOT 2 earnings block
    - Fixed chat_id vs peer_id mismatch
    - Added FloodWaitError handling, connection retries, graceful shutdown
"""

import os
import re
import sys
import io
import zipfile
import asyncio
import hashlib
import time
import aiohttp
from datetime import datetime, timedelta
from difflib import SequenceMatcher

from telethon import TelegramClient, events, utils, errors
from telethon.sessions import StringSession
from telethon.types import Channel, Chat

# OCR imports (optional - graceful fallback if not available)
try:
    from PIL import Image
    import pytesseract
    OCR_AVAILABLE = True
    # Heroku apt buildpack installs to /app/.apt/usr/bin
    tesseract_paths = [
        "/app/.apt/usr/bin/tesseract",
        "/usr/bin/tesseract",
        "/usr/local/bin/tesseract",
    ]
    for tp in tesseract_paths:
        if os.path.exists(tp):
            pytesseract.pytesseract.tesseract_cmd = tp
            break
    print(f"\u2705 OCR available (Tesseract)")
except ImportError:
    OCR_AVAILABLE = False
    print("\u26a0\ufe0f OCR not available (install Pillow + pytesseract)")

# ============================================================
# CONFIGURATION
# ============================================================
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH")
SESSION_STRING = os.environ.get("SESSION_STRING")
TARGET_CHANNEL = os.environ.get("TARGET_CHANNEL")       # @my_filtered_news
EARNINGS_CHANNEL = os.environ.get("EARNINGS_CHANNEL")   # @jason_earnings
VOLUME_ALERT_CHANNEL = os.environ.get("VOLUME_ALERT_CHANNEL")  # @alerts_forme
SIMILARITY_THRESHOLD = float(os.environ.get("SIMILARITY_THRESHOLD", "0.85"))

# 한국투자증권 API
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")
KIS_ACCOUNT_NO = os.environ.get("KIS_ACCOUNT_NO")
KIS_ACCOUNT_PROD = os.environ.get("KIS_ACCOUNT_PROD", "01")

# 거래대금 기준
VOLUME_THRESHOLD = int(os.environ.get("VOLUME_THRESHOLD", "1000000000"))
KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"

# KIS 종목정보파일 URLs
KIS_KOSPI_MST_URL = "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip"
KIS_KOSDAQ_MST_URL = "https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip"

# ============================================================
# CRYPTO FILTER KEYWORDS (BOT 1)
# ============================================================
CRYPTO_KEYWORDS = [
    "bitcoin", "btc", "ethereum", "eth", "crypto", "cryptocurrency",
    "blockchain", "defi", "nft", "altcoin", "altcoins",
    "coin", "coins", "token", "tokens",
    "binance", "coinbase", "upbit", "bithumb", "korbit",
    "ripple", "xrp", "solana", "sol", "dogecoin", "doge",
    "usdt", "usdc", "stablecoin", "web3", "metaverse",
    "airdrop", "staking", "mining", "wallet", "ledger",
    "metamask", "uniswap", "pancakeswap",
    "cardano", "ada", "polkadot", "dot", "avalanche", "avax",
    "bnb", "tron", "trx", "shiba", "pepe",
    "\ube44\ud2b8\ucf54\uc778", "\uc774\ub354\ub9ac\uc6c0", "\uac00\uc0c1\ud654\ud3d0", "\uc554\ud638\ud654\ud3d0", "\ucf54\uc778", "\ud1a0\ud070",
    "\ube14\ub85d\uccb4\uc778", "\ub514\ud30c\uc774", "\uc564\uc5d0\ud504\ud2f0", "\uc54c\ud2b8\ucf54\uc778",
    "\uc5c5\ube44\ud2b8", "\ube57\uc378", "\ub9ac\ud50c", "\uc194\ub77c\ub098", "\ub3c4\uc9c0\ucf54\uc778",
    "\uc2a4\ud14c\uc774\ud0b9", "\ucc44\uad74", "\uc9c0\uac11", "\ucf54\uc778\ub9c8\ucf13", "\uac00\uc0c1\uc790\uc0b0",
    "\ub514\uc9c0\ud138\uc790\uc0b0", "\ubc14\uc774\ub09c\uc2a4", "\uc5d0\uc5b4\ub4dc\ub86d",
]

# ============================================================
# 실적 + 공시 ANCHOR KEYWORDS (BOT 2) - context indicators
# ============================================================
EARNINGS_ANCHOR_KEYWORDS = [
    "공시", "공시내용", "수시공시", "주요공시",
    "실적발표", "실적공시", "잠정실적", "잠정치", "확정치",
    "사업보고서", "분기보고서", "반기보고서",
    "연결기준", "별도기준",
    "실적", "잠정실적", "실적시즌",
    "실적쇼크", "실적서프라이즈", "어닝쇼크", "어닝서프라이즈",
]

# ============================================================
# 실적 + 공시 DATA KEYWORDS (BOT 2) - financial data points
# ============================================================
EARNINGS_DATA_KEYWORDS = [
    "영업이익", "당기순이익", "순이익", "매출액", "매출",
    "영업손실", "순손실", "당기순손실",
    "적자전환", "흑자전환", "적자지속", "흑자지속",
    "매출총이익", "EBITDA", "EPS", "BPS",
    "영업이익률", "순이익률",
    "컨센서스",
    "전년대비", "전분기대비", "YoY", "QoQ",
    "1분기", "2분기", "3분기", "4분기",
    "1Q", "2Q", "3Q", "4Q",
    "반기실적", "연간실적",
    "ROE", "ROA", "PER", "PBR",
    "판매량", "판매실적",
    "수주", "수주잔고", "수주액",
    "이익", "순익",
]

# ============================================================
# HIGH-CONFIDENCE EARNINGS KEYWORDS (BOT 2) - bypass anchor requirement
# These keywords, when appearing with numbers, strongly indicate financial data
# ============================================================
HIGH_CONFIDENCE_EARNINGS_KEYWORDS = [
    "영업이익", "당기순이익", "순이익", "순익", "매출액",
    "영업손실", "순손실", "당기순손실", "매출",
]
FINANCIAL_NUMBER_PATTERN = re.compile(r'[\d,]+\s*(억|조|원|백만|천만)')

# ============================================================
# DYNAMIC STOCK UNIVERSE (replaces hardcoded STOCK_MAP)
# ============================================================
STOCK_CODE_PATTERN = re.compile(r'\b(\d{6})\b')

class StockUniverse:
    """
    Downloads and parses KIS master files (kospi + kosdaq) to build
    a complete name->code mapping for ALL listed Korean stocks.
    Refreshes every 24 hours.
    """
    def __init__(self):
        self.name_to_code = {}   # {"\uc0bc\uc131\uc804\uc790": "005930", ...}
        self.code_to_name = {}   # {"005930": "\uc0bc\uc131\uc804\uc790", ...}
        self.all_codes = set()
        self.last_refresh = 0
        self.refresh_interval = 86400  # 24 hours

    async def load(self, session=None):
        """Download and parse KIS master files."""
        close_session = False
        if session is None:
            session = aiohttp.ClientSession()
            close_session = True

        try:
            name_to_code = {}
            code_to_name = {}

            for url, market in [
                (KIS_KOSPI_MST_URL, "KOSPI"),
                (KIS_KOSDAQ_MST_URL, "KOSDAQ"),
            ]:
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                        if resp.status != 200:
                            print(f"\u26a0\ufe0f Failed to download {market} master: HTTP {resp.status}")
                            continue
                        data = await resp.read()

                    with zipfile.ZipFile(io.BytesIO(data)) as zf:
                        for filename in zf.namelist():
                            raw = zf.read(filename)
                            # KIS MST files use cp949 encoding
                            try:
                                text = raw.decode("cp949", errors="ignore")
                            except Exception:
                                text = raw.decode("utf-8", errors="ignore")

                            lines = text.strip().split("\n")
                            for line in lines:
                                line = line.strip()
                                if not line:
                                    continue
                                # MST format: first 9 chars = short code (padded),
                                # but actual format varies. Parse by extracting 6-digit code
                                # and Korean name from the line.
                                # Common format: code(9) + name(40) + ...
                                try:
                                    # Extract 6-digit code (skip leading spaces/zeros padding)
                                    code_part = line[:9].strip()
                                    # Find the 6-digit numeric code
                                    code_match = re.search(r'(\d{6})', code_part)
                                    if not code_match:
                                        continue
                                    code = code_match.group(1)

                                    # Extract Korean name (next ~40 chars after code section)
                                    name_part = line[9:49].strip()
                                    if not name_part:
                                        continue

                                    # Clean name: remove trailing spaces, special markers
                                    name = name_part.strip()
                                    if not name:
                                        continue

                                    name_to_code[name] = code
                                    code_to_name[code] = name
                                except Exception:
                                    continue

                    print(f"\u2705 {market} master loaded: {len([c for c in code_to_name if code_to_name[c]])} stocks")

                except Exception as e:
                    print(f"\u274c Error loading {market} master: {e}")

            if name_to_code:
                self.name_to_code = name_to_code
                self.code_to_name = code_to_name
                self.all_codes = set(code_to_name.keys())
                self.last_refresh = time.time()
                print(f"\U0001f4ca Stock universe: {len(self.name_to_code)} names, {len(self.all_codes)} codes")
            else:
                print("\u26a0\ufe0f Stock universe empty - master files may have changed format")

        finally:
            if close_session:
                await session.close()

    async def ensure_fresh(self, session=None):
        """Refresh if stale (older than 24 hours)."""
        if time.time() - self.last_refresh > self.refresh_interval:
            print("\U0001f504 Refreshing stock universe...")
            await self.load(session)

    def lookup_code(self, name):
        """Look up stock code by exact name."""
        return self.name_to_code.get(name)

    def lookup_name(self, code):
        """Look up stock name by code."""
        return self.code_to_name.get(code)

    def find_stocks_in_text(self, text):
        """
        Find all stock codes mentioned in text.
        Checks both direct 6-digit codes and stock names.
        Returns: list of stock codes
        """
        if not text:
            return []
        found = set()

        # 1) Direct 6-digit code matches
        for m in STOCK_CODE_PATTERN.finditer(text):
            code = m.group()
            if code != "000000" and code in self.all_codes:
                found.add(code)

        # 2) Stock name matches (longest match first to avoid partial matches)
        sorted_names = sorted(self.name_to_code.keys(), key=len, reverse=True)
        text_upper = text.upper()
        for name in sorted_names:
            if len(name) < 2:
                continue  # Skip very short names to avoid false positives
            if name in text or name.upper() in text_upper:
                code = self.name_to_code[name]
                if code:
                    found.add(code)

        return list(found)


# Global stock universe instance
stock_universe = StockUniverse()


# ============================================================
# OCR: Extract text from images
# ============================================================
async def extract_text_from_image(client, message):
    """
    Download image from Telegram message and run OCR.
    Returns extracted text or empty string.
    """
    if not OCR_AVAILABLE:
        return ""
    if not message.media:
        return ""

    try:
        # Download the image to memory
        image_bytes = await client.download_media(message, bytes)
        if not image_bytes:
            return ""

        # Open with PIL
        img = Image.open(io.BytesIO(image_bytes))

        # Run Tesseract OCR with Korean + English
        text = pytesseract.image_to_string(img, lang="kor+eng")
        text = text.strip()

        if text:
            print(f"\U0001f50d OCR extracted {len(text)} chars from image")
        return text

    except Exception as e:
        print(f"\u26a0\ufe0f OCR error: {e}")
        return ""


# ============================================================
# \ud55c\uad6d\ud22c\uc790\uc99d\uad8c API
# ============================================================
class KISApi:
    def __init__(self):
        self.access_token = None
        self.token_expires = 0
        self.session = None

    async def ensure_session(self):
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(timeout=timeout)

    async def get_token(self):
        now = time.time()
        if self.access_token and now < self.token_expires:
            return self.access_token
        await self.ensure_session()
        url = f"{KIS_BASE_URL}/oauth2/tokenP"
        body = {
            "grant_type": "client_credentials",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
        }
        try:
            async with self.session.post(url, json=body) as resp:
                data = await resp.json()
            if "access_token" in data:
                self.access_token = data["access_token"]
                self.token_expires = now + 85000
                print(f"\U0001f511 KIS \ud1a0\ud070 \ubc1c\uae09 \uc131\uacf5")
                return self.access_token
            else:
                print(f"\u274c KIS \ud1a0\ud070 \uc2e4\ud328: {data}")
                return None
        except Exception as e:
            print(f"\u274c KIS \ud1a0\ud070 \uc5d0\ub7ec: {e}")
            return None

    async def get_stock_price(self, stock_code):
        """\ud604\uc7ac\uac00 \uc870\ud68c"""
        token = await self.get_token()
        if not token:
            return None
        await self.ensure_session()
        url = f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
        headers = {
            "authorization": f"Bearer {token}",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
            "tr_id": "FHKST01010100",
            "Content-Type": "application/json; charset=utf-8",
        }
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": stock_code,
        }
        try:
            async with self.session.get(url, headers=headers, params=params) as resp:
                data = await resp.json()
            if data.get("rt_cd") == "0":
                out = data.get("output", {})
                return {
                    "name": out.get("hts_kor_isnm", stock_code),
                    "price": int(out.get("stck_prpr", "0")),
                    "change_rate": out.get("prdy_ctrt", "0"),
                    "change_sign": out.get("prdy_vrss_sign", "3"),
                    "high_price": int(out.get("stck_hgpr", "0")),
                    "low_price": int(out.get("stck_lwpr", "0")),
                    "open_price": int(out.get("stck_oprc", "0")),
                    "prev_close": int(out.get("stck_sdpr", "0")),
                    "acml_tr_pbmn": int(out.get("acml_tr_pbmn", "0")),
                    "acml_vol": int(out.get("acml_vol", "0")),
                    "per": out.get("per", "N/A"),
                    "pbr": out.get("pbr", "N/A"),
                    "w52_hgpr": int(out.get("w52_hgpr", "0")),
                    "w52_lwpr": int(out.get("w52_lwpr", "0")),
                }
            else:
                print(f"\u26a0\ufe0f \uc2dc\uc138\uc2e4\ud328 [{stock_code}]: {data.get('msg1', '')}")
                return None
        except asyncio.TimeoutError:
            print(f"\u26a0\ufe0f \uc2dc\uc138 \ud0c0\uc784\uc544\uc6c3 [{stock_code}]")
            return None
        except Exception as e:
            print(f"\u274c \uc2dc\uc138\uc5d0\ub7ec [{stock_code}]: {e}")
            return None

    async def get_rolling_volume(self, stock_code, minutes=20):
        """\ucd5c\uadfc N\ubd84 rolling \uac70\ub798\ub300\uae08 \uc870\ud68c."""
        token = await self.get_token()
        if not token:
            return 0
        await self.ensure_session()
        url = f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
        headers = {
            "authorization": f"Bearer {token}",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
            "tr_id": "FHKST03010200",
            "Content-Type": "application/json; charset=utf-8",
        }
        now = datetime.now()
        params = {
            "FID_ETC_CLS_CODE": "",
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": stock_code,
            "FID_INPUT_HOUR_1": now.strftime("%H%M%S"),
            "FID_PW_DATA_INCU_YN": "N",
        }
        try:
            async with self.session.get(url, headers=headers, params=params) as resp:
                data = await resp.json()
            if data.get("rt_cd") == "0":
                items = data.get("output2", [])
                total = 0
                count = 0
                for item in items:
                    if count >= minutes:
                        break
                    vol = int(item.get("cntg_vol", "0"))
                    price = int(item.get("stck_prpr", "0"))
                    total += vol * price
                    count += 1
                return total
            else:
                print(f"\u26a0\ufe0f \ubd84\ubd09\uc2e4\ud328 [{stock_code}]: {data.get('msg1', '')}")
                return 0
        except asyncio.TimeoutError:
            print(f"\u26a0\ufe0f \ubd84\ubd09 \ud0c0\uc784\uc544\uc6c3 [{stock_code}]")
            return 0
        except Exception as e:
            print(f"\u274c \ubd84\ubd09\uc5d0\ub7ec [{stock_code}]: {e}")
            return 0

    async def get_daily_prices(self, stock_code, count=60):
        """\uc77c\ubcc4 \uc885\uac00 \uc870\ud68c (\ucd5c\uadfc count\uc77c)."""
        token = await self.get_token()
        if not token:
            return []
        await self.ensure_session()
        url = f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        headers = {
            "authorization": f"Bearer {token}",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
            "tr_id": "FHKST03010100",
            "Content-Type": "application/json; charset=utf-8",
        }
        today = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": stock_code,
            "FID_INPUT_DATE_1": start_date,
            "FID_INPUT_DATE_2": today,
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": "0",
        }
        try:
            async with self.session.get(url, headers=headers, params=params) as resp:
                data = await resp.json()
            if data.get("rt_cd") == "0":
                items = data.get("output2", [])
                closes = []
                for item in items[:count]:
                    close = int(item.get("stck_clpr", "0"))
                    if close > 0:
                        closes.append(close)
                return closes
            else:
                print(f"\u26a0\ufe0f \uc77c\ubd09\uc2e4\ud328 [{stock_code}]: {data.get('msg1', '')}")
                return []
        except asyncio.TimeoutError:
            print(f"\u26a0\ufe0f \uc77c\ubd09 \ud0c0\uc784\uc544\uc6c3 [{stock_code}]")
            return []
        except Exception as e:
            print(f"\u274c \uc77c\ubd09\uc5d0\ub7ec [{stock_code}]: {e}")
            return []

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()


# ============================================================
# RISK LEVEL & MOVING AVERAGE HELPERS
# ============================================================
def compute_risk_level(price_info):
    try:
        change_rate = abs(float(price_info.get("change_rate", "0")))
        price = price_info.get("price", 0)
        w52_high = price_info.get("w52_hgpr", 0)
        w52_low = price_info.get("w52_lwpr", 0)
        if w52_high > w52_low and w52_high > 0:
            position = (price - w52_low) / (w52_high - w52_low) * 100
        else:
            position = 50
        risk_score = 0
        if change_rate >= 10: risk_score += 3
        elif change_rate >= 5: risk_score += 2
        elif change_rate >= 3: risk_score += 1
        if position >= 90: risk_score += 2
        elif position >= 75: risk_score += 1
        if position <= 10: risk_score += 2
        elif position <= 25: risk_score += 1
        if risk_score >= 4:
            return "\U0001f534 HIGH", "\U0001f534"
        elif risk_score >= 2:
            return "\U0001f7e1 MEDIUM", "\U0001f7e1"
        else:
            return "\U0001f7e2 LOW", "\U0001f7e2"
    except Exception:
        return "\u2753 N/A", "\u2753"


def compute_ma_state(closes, current_price=None):
    try:
        if len(closes) < 5:
            return "\ub370\uc774\ud130 \ubd80\uc871", "\u2753"
        ma5 = sum(closes[:5]) / 5
        ma20 = sum(closes[:20]) / 20 if len(closes) >= 20 else None
        ma60 = sum(closes[:60]) / 60 if len(closes) >= 60 else None
        price = current_price if current_price else closes[0]
        parts = []
        signals = {"bullish": 0, "bearish": 0}
        if price > ma5:
            parts.append(f"5\uc77c\uc120({ma5:,.0f}) \uc704")
            signals["bullish"] += 1
        else:
            parts.append(f"5\uc77c\uc120({ma5:,.0f}) \uc544\ub798")
            signals["bearish"] += 1
        if ma20:
            if price > ma20:
                parts.append(f"20\uc77c\uc120({ma20:,.0f}) \uc704")
                signals["bullish"] += 1
            else:
                parts.append(f"20\uc77c\uc120({ma20:,.0f}) \uc544\ub798")
                signals["bearish"] += 1
        if ma60:
            if price > ma60:
                parts.append(f"60\uc77c\uc120({ma60:,.0f}) \uc704")
                signals["bullish"] += 1
            else:
                parts.append(f"60\uc77c\uc120({ma60:,.0f}) \uc544\ub798")
                signals["bearish"] += 1
        cross = ""
        if ma20 and len(closes) >= 21:
            prev_ma5 = sum(closes[1:6]) / 5
            prev_ma20 = sum(closes[1:21]) / 20
            if prev_ma5 <= prev_ma20 and ma5 > ma20:
                cross = "\U0001f31f \uace8\ub4e0\ud06c\ub85c\uc2a4(5/20)"
                signals["bullish"] += 2
            elif prev_ma5 >= prev_ma20 and ma5 < ma20:
                cross = "\U0001f480 \ub370\ub4dc\ud06c\ub85c\uc2a4(5/20)"
                signals["bearish"] += 2
        total_bull = signals["bullish"]
        total_bear = signals["bearish"]
        if total_bull >= 4:
            state = "\U0001f7e2 \uac15\uc138 (Strong Bullish)"
        elif total_bull > total_bear:
            state = "\U0001f7e2 \uc0c1\uc2b9\ucd94\uc138 (Bullish)"
        elif total_bear >= 4:
            state = "\U0001f534 \uc57d\uc138 (Strong Bearish)"
        elif total_bear > total_bull:
            state = "\U0001f534 \ud558\ub77d\ucd94\uc138 (Bearish)"
        else:
            state = "\U0001f7e1 \ud6a1\ubcf4 (Neutral)"
        ma_detail = " | ".join(parts)
        if cross:
            ma_detail += f" | {cross}"
        return state, ma_detail
    except Exception:
        return "\u2753 \uacc4\uc0b0 \ubd88\uac00", ""


# ============================================================
# HELPER FUNCTIONS
# ============================================================
def contains_crypto_keyword(text):
    if not text:
        return False
    lower = text.lower()
    return any(kw in lower for kw in CRYPTO_KEYWORDS)


def contains_earnings_keyword(text):
    """
    Two-layer filter with high-confidence bypass.
    1) Standard: requires BOTH anchor + data keyword
    2) Bypass: high-confidence keywords (영업이익, 순이익, etc.) with a number pattern
       strongly indicate financial data even without an anchor keyword.
    """
    if not text:
        return False
    lower = text.lower()

    # Standard two-layer check
    has_anchor = any(kw.lower() in lower for kw in EARNINGS_ANCHOR_KEYWORDS)
    has_data = any(kw.lower() in lower for kw in EARNINGS_DATA_KEYWORDS)
    if has_anchor and has_data:
        return True

    # High-confidence bypass: keyword + number pattern (e.g., "영업이익 1조2천억")
    has_high_conf = any(kw.lower() in lower for kw in HIGH_CONFIDENCE_EARNINGS_KEYWORDS)
    has_number = bool(FINANCIAL_NUMBER_PATTERN.search(text))
    if has_high_conf and has_number:
        return True

    return False


# ============================================================
# DUPLICATE DETECTOR
# ============================================================
class DuplicateDetector:
    def __init__(self, threshold=0.85, max_history=500, ttl_hours=6):
        self.threshold = threshold
        self.max_history = max_history
        self.ttl_seconds = ttl_hours * 3600
        self.seen_hashes = {}
        self.seen_texts = []
        self.stats = {"total": 0, "unique": 0, "duplicate": 0, "skipped": 0}

    def _clean_old(self):
        now = time.time()
        old_count = len(self.seen_hashes)
        self.seen_hashes = {
            h: t for h, t in self.seen_hashes.items()
            if now - t < self.ttl_seconds
        }
        cutoff = max(0, len(self.seen_texts) - self.max_history)
        self.seen_texts = self.seen_texts[cutoff:]
        cleaned = old_count - len(self.seen_hashes)
        if cleaned:
            print(f"\U0001f9f9 Cleaned {cleaned} old entries")

    def _get_hash(self, text):
        cleaned = " ".join(text.lower().split())
        return hashlib.md5(cleaned.encode()).hexdigest()

    def is_duplicate(self, text):
        self.stats["total"] += 1
        if not text or not text.strip():
            self.stats["skipped"] += 1
            return True
        self._clean_old()
        text_hash = self._get_hash(text)
        if text_hash in self.seen_hashes:
            self.stats["duplicate"] += 1
            return True
        if len(text.strip()) > 30:
            for old_text in self.seen_texts:
                ratio = SequenceMatcher(None, text.lower(), old_text.lower()).ratio()
                if ratio >= self.threshold:
                    self.stats["duplicate"] += 1
                    return True
        self.seen_hashes[text_hash] = time.time()
        self.seen_texts.append(text)
        self.stats["unique"] += 1
        return False

    def get_stats(self):
        return self.stats


class AlertCooldown:
    def __init__(self, cooldown_minutes=30):
        self.cooldown = cooldown_minutes * 60
        self.last_alert = {}

    def can_alert(self, stock_code):
        now = time.time()
        if now - self.last_alert.get(stock_code, 0) >= self.cooldown:
            self.last_alert[stock_code] = now
            return True
        return False

    def reset(self, stock_code):
        self.last_alert.pop(stock_code, None)


async def safe_forward(client, channel, message, max_retries=3):
    for attempt in range(max_retries):
        try:
            await client.forward_messages(channel, message)
            return True
        except errors.FloodWaitError as e:
            await asyncio.sleep(e.seconds + 1)
        except Exception:
            if attempt >= max_retries - 1:
                raise
    return False


async def safe_send(client, channel, text, max_retries=3, **kwargs):
    for attempt in range(max_retries):
        try:
            await client.send_message(channel, text, **kwargs)
            return True
        except errors.FloodWaitError as e:
            await asyncio.sleep(e.seconds + 1)
        except Exception:
            if attempt >= max_retries - 1:
                raise
    return False


# ============================================================
# MAIN
# ============================================================
async def main():
    print("=" * 50)
    print("  Telegram Monitor v7.5")
    print("  BOT1: Filter+Dedup (no crypto) -> @my_filtered_news")
    print("  BOT2: \uc2e4\uc801/\uacf5\uc2dc -> @jason_earnings")
    print("  BOT3: \uc885\ubaa9\ubcc4 \uc2dc\uc138/\uac70\ub798\ub300\uae08/RISK/MA + OCR -> @alerts_forme")
    print("=" * 50)

    if not all([API_ID, API_HASH, SESSION_STRING, TARGET_CHANNEL]):
        missing = []
        if not API_ID: missing.append("API_ID")
        if not API_HASH: missing.append("API_HASH")
        if not SESSION_STRING: missing.append("SESSION_STRING")
        if not TARGET_CHANNEL: missing.append("TARGET_CHANNEL")
        print(f"\u274c Missing env vars: {', '.join(missing)}")
        return

    kis_ok = all([KIS_APP_KEY, KIS_APP_SECRET, VOLUME_ALERT_CHANNEL])
    if not kis_ok:
        print("\u26a0\ufe0f KIS API or VOLUME_ALERT_CHANNEL not set -- stock alerts disabled")
    if not EARNINGS_CHANNEL:
        print("\u26a0\ufe0f EARNINGS_CHANNEL not set -- earnings filter disabled")

    kis = KISApi() if kis_ok else None
    detector = DuplicateDetector(threshold=SIMILARITY_THRESHOLD)
    cooldown = AlertCooldown(cooldown_minutes=30)

    # Load stock universe from KIS master files
    print("\U0001f4e5 Loading stock universe from KIS master files...")
    await stock_universe.load()
    if not stock_universe.all_codes:
        print("\u26a0\ufe0f Stock universe empty - falling back without name matching")

    client = TelegramClient(
        StringSession(SESSION_STRING),
        API_ID, API_HASH,
        connection_retries=5,
        retry_delay=2,
    )
    await client.start()
    me = await client.get_me()
    print(f"\u2705 Connected: {me.first_name} (@{me.username})")

    if kis:
        tok = await kis.get_token()
        if tok:
            print("\u2705 KIS API connected")
        else:
            print("\u26a0\ufe0f KIS API token failed -- stock alerts disabled")
            kis = None

    monitored_ids = set()
    exclude_ids = set()
    for ch in [TARGET_CHANNEL, EARNINGS_CHANNEL, VOLUME_ALERT_CHANNEL]:
        if not ch:
            continue
        try:
            ent = await client.get_entity(ch)
            exclude_ids.add(utils.get_peer_id(ent))
            print(f"  \u2705 Output channel: {ch}")
        except Exception as e:
            print(f"  \u274c Cannot find channel {ch}: {e}")

    async def refresh_dialogs():
        added = 0
        async for dialog in client.iter_dialogs():
            entity = dialog.entity
            try:
                peer_id = utils.get_peer_id(entity)
            except Exception:
                continue
            if peer_id in exclude_ids:
                continue
            if peer_id in monitored_ids:
                continue
            if isinstance(entity, Channel) and entity.broadcast:
                monitored_ids.add(peer_id)
                print(f"  \U0001f4fa Channel: {entity.title} (id: {peer_id})")
                added += 1
            elif isinstance(entity, Channel) and entity.megagroup:
                monitored_ids.add(peer_id)
                print(f"  \U0001f465 Megagroup: {entity.title} (id: {peer_id})")
                added += 1
            elif isinstance(entity, Chat):
                monitored_ids.add(peer_id)
                print(f"  \U0001f465 Chat: {entity.title} (id: {peer_id})")
                added += 1
        return added

    await refresh_dialogs()
    print(f"\U0001f4ca Monitoring {len(monitored_ids)} chats")
    print(f"\U0001f4ec All unique (no crypto) -> {TARGET_CHANNEL}")
    if EARNINGS_CHANNEL:
        print(f"\U0001f4c8 \uc2e4\uc801/\uacf5\uc2dc -> {EARNINGS_CHANNEL}")
    if kis:
        print(f"\U0001f6a8 \uc885\ubaa9 \uc54c\ub9bc (\uc2dc\uc138/\uac70\ub798\ub300\uae08/RISK/MA) -> {VOLUME_ALERT_CHANNEL}")
    if OCR_AVAILABLE:
        print("\U0001f50d OCR enabled: image stock detection active")

    bg_tasks = []

    async def print_stats():
        try:
            while True:
                await asyncio.sleep(600)
                stats = detector.get_stats()
                print(f"\U0001f4ca Stats: total={stats['total']} unique={stats['unique']} "
                      f"dup={stats['duplicate']} skip={stats['skipped']}")
        except asyncio.CancelledError:
            pass

    bg_tasks.append(asyncio.create_task(print_stats()))

    async def periodic_refresh():
        try:
            while True:
                await asyncio.sleep(1800)
                print("\U0001f504 Refreshing dialog list...")
                added = await refresh_dialogs()
                print(f"  \u2705 {added} new, total: {len(monitored_ids)}")
                # Also refresh stock universe if stale
                await stock_universe.ensure_fresh()
        except asyncio.CancelledError:
            pass

    bg_tasks.append(asyncio.create_task(periodic_refresh()))

    # --------------------------------------------------------
    @client.on(events.NewMessage())
    async def handler(event):
        try:
            chat = await event.get_chat()
            try:
                peer_id = utils.get_peer_id(chat)
            except Exception:
                return
            if peer_id in exclude_ids:
                return
            if peer_id not in monitored_ids:
                if isinstance(chat, (Channel, Chat)):
                    monitored_ids.add(peer_id)
                    print(f"  \U0001f195 Dynamically added: {getattr(chat, 'title', 'Unknown')}")
                else:
                    return

            chat_name = getattr(chat, "title", "Unknown")
            msg = event.message.text or ""
            has_media = event.message.media is not None
            is_media_only = not msg and has_media

            # ====== OCR: Extract text from images for stock detection ======
            ocr_text = ""
            if has_media and OCR_AVAILABLE and kis:
                ocr_text = await extract_text_from_image(client, event.message)

            # Combined text for stock detection (message text + OCR text)
            combined_text = msg
            if ocr_text:
                combined_text = f"{msg}\n{ocr_text}" if msg else ocr_text

            # ====== BOT 1: Media-only (no text, no OCR stocks) ======
            if is_media_only and not ocr_text:
                try:
                    await safe_forward(client, TARGET_CHANNEL, event.message)
                except Exception:
                    try:
                        icon = "\U0001f4fa" if isinstance(chat, Channel) and chat.broadcast else "\U0001f465"
                        await safe_send(client, TARGET_CHANNEL,
                            f"\U0001f4ce {icon}**{chat_name}** [\ubbf8\ub514\uc5b4 \uba54\uc2dc\uc9c0]",
                            link_preview=False)
                    except Exception:
                        pass
                # Still check for stocks via OCR below if ocr_text exists
                if not ocr_text:
                    return

            # ====== BOT 1: Text - filter crypto, dedup ======
            if msg:
                if contains_crypto_keyword(msg):
                    return
                if detector.is_duplicate(msg):
                    return

                print(f"\U0001f4e8 [{chat_name}] Unique msg ({len(msg)} chars)")

                forwarded = False
                try:
                    await safe_forward(client, TARGET_CHANNEL, event.message)
                    forwarded = True
                    print(f"  \u2705 Forwarded to {TARGET_CHANNEL}")
                except Exception:
                    try:
                        icon = "\U0001f4fa" if isinstance(chat, Channel) and chat.broadcast else "\U0001f465"
                        await safe_send(client, TARGET_CHANNEL,
                            f"{icon}**{chat_name}**\n{msg}",
                            link_preview=False)
                        forwarded = True
                    except Exception:
                        pass
                if not forwarded:
                    print(f"  \u274c FAILED to deliver to {TARGET_CHANNEL}!")

            # ====== BOT 2: \uc2e4\uc801/\uacf5\uc2dc ======
            if EARNINGS_CHANNEL and (msg or ocr_text) and contains_earnings_keyword(combined_text):
                matched = [kw for kw in EARNINGS_ANCHOR_KEYWORDS + EARNINGS_DATA_KEYWORDS + HIGH_CONFIDENCE_EARNINGS_KEYWORDS if kw.lower() in combined_text.lower()][:5]
                print(f"  \U0001f4c8 \uc2e4\uc801/\uacf5\uc2dc: {matched}")
                try:
                    await safe_forward(client, EARNINGS_CHANNEL, event.message)
                    print(f"  \u2705 -> {EARNINGS_CHANNEL}")
                except Exception:
                    try:
                        await safe_send(client, EARNINGS_CHANNEL,
                            f"\U0001f4c8 **[\uc2e4\uc801/\uacf5\uc2dc]** {chat_name}\n"
                            f"\U0001f511 {', '.join(matched)}\n"
                            f"{combined_text[:500]}",
                            link_preview=False)
                    except Exception:
                        pass

            # ====== BOT 3: \uc885\ubaa9\ubcc4 \uc2dc\uc138/\uac70\ub798\ub300\uae08/RISK/MA \uc54c\ub9bc ======
            # Uses combined_text (msg + OCR) for stock detection
            if kis and combined_text:
                codes = stock_universe.find_stocks_in_text(combined_text)

                if ocr_text and codes:
                    print(f"  \U0001f50d OCR detected stocks: {codes}")

                for code in codes:
                    if not cooldown.can_alert(code):
                        continue

                    # 1) \ud604\uc7ac\uac00 \uc870\ud68c
                    price_info = await kis.get_stock_price(code)
                    if not price_info:
                        cooldown.reset(code)
                        continue

                    # Filter: skip if cumulative trading value < 50억
                    if price_info.get("acml_tr_pbmn", 0) < 5_000_000_000:
                        cooldown.reset(code)
                        continue

                    api_name = price_info.get("name", "")
                    universe_name = stock_universe.lookup_name(code) or ""
                    # Use API name if it's a real name (not just the code), otherwise use universe name
                    if api_name and api_name != code and not api_name.isdigit():
                        name = api_name
                    elif universe_name:
                        name = universe_name
                    else:
                        name = code

                    # 2) 20\ubd84 rolling \uac70\ub798\ub300\uae08
                    rolling_vol = await kis.get_rolling_volume(code, minutes=20)

                    # 3) \uc77c\ubcc4 \uc885\uac00 (MA \uacc4\uc0b0\uc6a9)
                    daily_closes = await kis.get_daily_prices(code, count=60)

                    # 4) RISK \ud310\uc815
                    risk_label, risk_emoji = compute_risk_level(price_info)

                    # 5) \uc774\ub3d9\ud3c9\uade0 \uc0c1\ud0dc
                    ma_state, ma_detail = compute_ma_state(daily_closes, price_info["price"])

                    # 6) \ub4f1\ub77d \ubc29\ud5a5
                    change_val = float(price_info.get("change_rate", "0"))
                    if change_val > 0:
                        direction = "\U0001f53a"
                    elif change_val < 0:
                        direction = "\U0001f53b"
                    else:
                        direction = "\u25ab"

                    source_type = "[OCR]" if (ocr_text and not msg) else ""
                    print(f"  \U0001f4b0 {source_type}[{code}] {name} {price_info['price']:,}\uc6d0 "
                          f"({change_val:+.2f}%) 20\ubd84\uac70\ub798\ub300\uae08: {rolling_vol/100_000_000:.2f}\uc5b5")

                    # \uc54c\ub9bc \uba54\uc2dc\uc9c0 \uad6c\uc131
                    alert_lines = [
                        f"\U0001f514 **\uc885\ubaa9 \uc54c\ub9bc ({name})**",
                        f"",
                        f"\U0001f4cc **{name}** ({code})",
                        f"\U0001f4b0 \ud604\uc7ac\uac00: {price_info['price']:,}\uc6d0 {direction} {change_val:+.2f}%",
                        f"\U0001f4c8 \uc2dc\uac00: {price_info.get('open_price', 0):,} | \uace0\uac00: {price_info.get('high_price', 0):,} | \uc800\uac00: {price_info.get('low_price', 0):,}",
                        f"\U0001f4ca \uac70\ub798\ub7c9: {price_info.get('acml_vol', 0):,}\uc8fc",
                        f"\U0001f4b5 \ub204\uc801\uac70\ub798\ub300\uae08: {price_info.get('acml_tr_pbmn', 0)/100_000_000:.1f}\uc5b5",
                        f"\U0001f4b5 20\ubd84 \uac70\ub798\ub300\uae08: {rolling_vol/100_000_000:.1f}\uc5b5",
                        f"",
                        f"\u26a0\ufe0f RISK: {risk_label}",
                        f"\U0001f4c9 \uc774\ub3d9\ud3c9\uade0: {ma_state}",
                    ]
                    if ma_detail:
                        alert_lines.append(f"  \u27a1 {ma_detail}")

                    w52h = price_info.get("w52_hgpr", 0)
                    w52l = price_info.get("w52_lwpr", 0)
                    if w52h and w52l:
                        pos_pct = ((price_info["price"] - w52l) / (w52h - w52l) * 100) if w52h > w52l else 0
                        alert_lines.append(f"\U0001f4cd 52\uc8fc: {w52l:,} ~ {w52h:,} (\ud604\uc7ac {pos_pct:.0f}% \uc704\uce58)")

                    alert_lines.extend([
                        f"",
                        f"\U0001f4ac \ucd9c\ucc98: {chat_name}" + (f" \U0001f50d(OCR)" if ocr_text and code not in (stock_universe.find_stocks_in_text(msg) if msg else []) else ""),
                        f"\u23f0 {datetime.now().strftime('%H:%M:%S')}",
                    ])

                    alert = "\n".join(alert_lines)

                    try:
                        await safe_send(client, VOLUME_ALERT_CHANNEL,
                            alert, link_preview=False)
                        print(f"  \U0001f6a8 ALERT: {name} ({code}) \u2705")
                    except Exception as e:
                        print(f"  \u274c Alert failed: {e}")

                    await asyncio.sleep(0.3)

        except errors.FloodWaitError as e:
            await asyncio.sleep(e.seconds + 1)
        except Exception as e:
            print(f"  \u274c Handler error: {e}")
            import traceback
            traceback.print_exc()

    print(f"\U0001f3a7 Listening... (v7.5)")

    try:
        await client.run_until_disconnected()
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        for task in bg_tasks:
            task.cancel()
        await asyncio.gather(*bg_tasks, return_exceptions=True)
        if kis:
            await kis.close()
        print("\u2705 Cleanup complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bye!")
    except Exception as e:
        print(f"\u274c Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
