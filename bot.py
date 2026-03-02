"""
Telegram Channel & Group Monitor v7.8
======================================
BOT 1: All unique messages (no crypto/coin) -> TARGET_CHANNEL (@my_filtered_news)
BOT 2: 실적/공시 keyword messages -> EARNINGS_CHANNEL (@jason_earnings)
BOT 3: 종목 언급 시 -> 현재가, 거래대금, RISK, 이동평균 상태 알림 -> VOLUME_ALERT_CHANNEL (@alerts_forme)

v7.8 Changes:
  - Fixed master file parsing: now uses official KIS structure
    (code[0:9], skip standard code[9:21], name[21:len-228/222])
  - Fixed KIS API market code: KOSDAQ stocks now queried with "Q" instead of "J"
    (stored per-code via code_to_market dict)
  - Removed extract_stock_name ISIN hack (no longer needed with correct parsing)
  - Handles duplicate stock names across KOSPI/KOSDAQ by appending market suffix

v7.7 Changes:
  - Fixed BOT 1: media-only messages now checked via OCR against crypto filter & dedup
  - Fixed dedup to check combined_text (msg + OCR) not just msg
  - Fixed BOT 3: pre-compute text_stock_codes outside per-code loop
  - Fixed get_rolling_volume: validates candle timestamps within N-minute window
  - Fixed compute_ma_state: validates date ordering of daily closes
  - Lowered simhash short-text threshold from 30 to 15 chars
  - Pinned all dependency versions in requirements.txt
  - Added SIGTERM signal handling for graceful Docker/Heroku shutdown
  - periodic_refresh now prunes stale monitored_ids for left channels
  - Dockerfile runs as non-root user
  - Lighter: consolidated keyword sets, frozenset for O(1) lookups,
    removed redundant imports, smaller memory footprint
"""

import os
import re
import sys
import io
import signal
import zipfile
import asyncio
import hashlib
import time
import aiohttp
from datetime import datetime, timedelta
from telethon import TelegramClient, events, utils, errors
from telethon.sessions import StringSession
from telethon.types import Channel, Chat

# OCR imports (optional)
try:
    from PIL import Image
    import pytesseract
    OCR_AVAILABLE = True
    for tp in ("/app/.apt/usr/bin/tesseract", "/usr/bin/tesseract", "/usr/local/bin/tesseract"):
        if os.path.exists(tp):
            pytesseract.pytesseract.tesseract_cmd = tp
            break
    print("\u2705 OCR available (Tesseract)")
except ImportError:
    OCR_AVAILABLE = False
    print("\u26a0\ufe0f OCR not available (install Pillow + pytesseract)")

# ============================================================
# CONFIGURATION
# ============================================================
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH")
SESSION_STRING = os.environ.get("SESSION_STRING")
TARGET_CHANNEL = os.environ.get("TARGET_CHANNEL")
EARNINGS_CHANNEL = os.environ.get("EARNINGS_CHANNEL")
VOLUME_ALERT_CHANNEL = os.environ.get("VOLUME_ALERT_CHANNEL")
SIMILARITY_THRESHOLD = float(os.environ.get("SIMILARITY_THRESHOLD", "0.85"))

KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")
VOLUME_THRESHOLD = int(os.environ.get("VOLUME_THRESHOLD", "1000000000"))
KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"

KIS_KOSPI_MST_URL = "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip"
KIS_KOSDAQ_MST_URL = "https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip"

# Tail lengths per official KIS header structure
_KOSPI_TAIL_LEN = 228
_KOSDAQ_TAIL_LEN = 222

# ============================================================
# CRYPTO FILTER KEYWORDS (BOT 1) - frozenset for O(1) lookup
# ============================================================
CRYPTO_KEYWORDS = frozenset([
    "bitcoin", "btc", "ethereum", "eth", "crypto", "cryptocurrency",
    "blockchain", "defi", "nft", "altcoin", "altcoins",
    "binance", "coinbase", "upbit", "bithumb", "korbit",
    "ripple", "xrp", "solana", "dogecoin", "doge",
    "usdt", "usdc", "stablecoin", "web3", "airdrop", "staking",
    "ledger", "metamask", "uniswap", "pancakeswap",
    "cardano", "ada", "polkadot", "dot", "avalanche", "avax",
    "bnb", "tron", "trx", "shiba", "pepe",
    "비트코인", "이더리움", "가상화폐", "암호화폐", "블록체인",
    "디파이", "엔에프티", "알트코인", "업비트", "빗썸",
    "리플", "솔라나", "도지코인", "스테이킹", "채굴",
    "코인마켓", "가상자산", "디지털자산", "바이낸스", "에어드롭",
])

# ============================================================
# 실적 + 공시 ANCHOR KEYWORDS (BOT 2)
# ============================================================
EARNINGS_ANCHOR_KEYWORDS = frozenset([
    "공시", "공시내용", "수시공시", "주요공시",
    "실적발표", "실적공시", "잠정실적", "잠정치", "확정치",
    "사업보고서", "분기보고서", "반기보고서",
    "연결기준", "별도기준",
    "실적", "실적시즌", "실적쇼크", "실적서프라이즈",
    "어닝쇼크", "어닝서프라이즈",
])

# ============================================================
# 실적 + 공시 DATA KEYWORDS (BOT 2)
# ============================================================
EARNINGS_DATA_KEYWORDS = frozenset([
    "영업이익", "당기순이익", "순이익", "매출액", "매출",
    "영업손실", "순손실", "당기순손실",
    "적자전환", "흑자전환", "적자지속", "흑자지속",
    "매출총이익", "EBITDA", "EPS", "BPS",
    "영업이익률", "순이익률", "컨센서스",
    "전년대비", "전분기대비", "YoY", "QoQ",
    "1분기", "2분기", "3분기", "4분기",
    "1Q", "2Q", "3Q", "4Q",
    "반기실적", "연간실적",
    "ROE", "ROA", "PER", "PBR",
    "판매량", "판매실적", "수주", "수주잔고", "수주액",
    "이익", "순익",
])

# ============================================================
# HIGH-CONFIDENCE EARNINGS KEYWORDS (BOT 2)
# ============================================================
HIGH_CONFIDENCE_EARNINGS_KEYWORDS = frozenset([
    "영업이익", "당기순이익", "순이익", "순익", "매출액",
    "영업손실", "순손실", "당기순손실", "매출",
])

# Pre-compile lowercase sets for fast matching
_CRYPTO_LOWER = frozenset(k.lower() for k in CRYPTO_KEYWORDS)
_ANCHOR_LOWER = frozenset(k.lower() for k in EARNINGS_ANCHOR_KEYWORDS)
_DATA_LOWER = frozenset(k.lower() for k in EARNINGS_DATA_KEYWORDS)
_HIGH_CONF_LOWER = frozenset(k.lower() for k in HIGH_CONFIDENCE_EARNINGS_KEYWORDS)
_ALL_EARNINGS_LOWER = _ANCHOR_LOWER | _DATA_LOWER | _HIGH_CONF_LOWER

FINANCIAL_NUMBER_PATTERN = re.compile(r'[\d,]+\s*(억|조|원|백만|천만)')
STOCK_CODE_PATTERN = re.compile(r'\b(\d{6})\b')

# ============================================================
# DYNAMIC STOCK UNIVERSE (v7.8: correct KIS master file parsing)
# ============================================================
class StockUniverse:
    """
    Downloads KIS master files to build name<->code mappings.
    v7.8: Uses official KIS file structure:
      - KOSPI row: short_code[0:9] + std_code[9:21] + name[21:len-228] + tail[228]
      - KOSDAQ row: short_code[0:9] + std_code[9:21] + name[21:len-222] + tail[222]
    Also stores market type (J=KOSPI, Q=KOSDAQ) per code for correct API calls.
    """

    def __init__(self):
        self.name_to_code = {}
        self.code_to_name = {}
        self.code_to_market = {}   # {"005930": "J", "293490": "Q", ...}
        self.all_codes = set()
        self.last_refresh = 0
        self.refresh_interval = 86400
        self._sorted_names = []

    def _parse_master_file(self, raw_bytes, tail_len, market_code):
        """Parse a single KIS master file (KOSPI or KOSDAQ).

        Args:
            raw_bytes: raw file bytes (cp949 encoded)
            tail_len: fixed-width tail length (228 for KOSPI, 222 for KOSDAQ)
            market_code: "J" for KOSPI, "Q" for KOSDAQ

        Returns:
            (name_to_code, code_to_name, code_to_market) dicts
        """
        name_to_code = {}
        code_to_name = {}
        code_to_market = {}
        text = raw_bytes.decode("cp949", errors="ignore")
        for row in text.strip().split("\n"):
            if not row or len(row) <= tail_len + 21:
                continue
            # Official structure: front part is row[0 : len(row)-tail_len]
            front = row[:len(row) - tail_len]
            short_code_raw = front[0:9].strip()
            # Extract 6-digit code from the 9-char short code field
            code_match = re.search(r'(\d{6})', short_code_raw)
            if not code_match:
                continue
            code = code_match.group(1)
            # Name starts at byte 21 (after short_code[0:9] + std_code[9:21])
            name = front[21:].strip()
            if not name:
                continue
            name_to_code[name] = code
            code_to_name[code] = name
            code_to_market[code] = market_code
        return name_to_code, code_to_name, code_to_market

    async def load(self, session=None):
        close_session = False
        if session is None:
            session = aiohttp.ClientSession()
            close_session = True
        try:
            all_name_to_code = {}
            all_code_to_name = {}
            all_code_to_market = {}
            for url, market, market_code, tail_len in [
                (KIS_KOSPI_MST_URL, "KOSPI", "J", _KOSPI_TAIL_LEN),
                (KIS_KOSDAQ_MST_URL, "KOSDAQ", "Q", _KOSDAQ_TAIL_LEN),
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
                            n2c, c2n, c2m = self._parse_master_file(raw, tail_len, market_code)
                            # Handle duplicate names across markets
                            for name, code in n2c.items():
                                if name in all_name_to_code and all_name_to_code[name] != code:
                                    # Name collision: append market suffix to both
                                    existing_code = all_name_to_code.pop(name)
                                    existing_market = all_code_to_market.get(existing_code, "J")
                                    suffixed_old = f"{name}({existing_market})"
                                    suffixed_new = f"{name}({market_code})"
                                    all_name_to_code[suffixed_old] = existing_code
                                    all_name_to_code[suffixed_new] = code
                                else:
                                    all_name_to_code[name] = code
                            all_code_to_name.update(c2n)
                            all_code_to_market.update(c2m)
                    print(f"\u2705 {market} master loaded: {len(c2n)} stocks")
                except Exception as e:
                    print(f"\u274c Error loading {market} master: {e}")
            if all_name_to_code:
                self.name_to_code = all_name_to_code
                self.code_to_name = all_code_to_name
                self.code_to_market = all_code_to_market
                self.all_codes = set(all_code_to_name.keys())
                self._sorted_names = sorted(
                    (n for n in all_name_to_code if len(n) >= 2),
                    key=len, reverse=True
                )
                self.last_refresh = time.time()
                print(f"\U0001f4ca Stock universe: {len(self.name_to_code)} names, {len(self.all_codes)} codes")
            else:
                print("\u26a0\ufe0f Stock universe empty - master files may have changed format")
        finally:
            if close_session:
                await session.close()

    async def ensure_fresh(self, session=None):
        if time.time() - self.last_refresh > self.refresh_interval:
            print("\U0001f504 Refreshing stock universe...")
            await self.load(session)

    def lookup_code(self, name):
        return self.name_to_code.get(name)

    def lookup_name(self, code):
        return self.code_to_name.get(code)

    def lookup_market(self, code):
        """Return market division code: 'J' for KOSPI, 'Q' for KOSDAQ."""
        return self.code_to_market.get(code, "J")

    def find_stocks_in_text(self, text):
        if not text:
            return []
        found = set()
        for m in STOCK_CODE_PATTERN.finditer(text):
            code = m.group()
            if code != "000000" and code in self.all_codes:
                found.add(code)
        text_upper = text.upper()
        for name in self._sorted_names:
            if name in text or name.upper() in text_upper:
                code = self.name_to_code.get(name)
                if code:
                    found.add(code)
        return list(found)

stock_universe = StockUniverse()

# ============================================================
# OCR
# ============================================================
async def extract_text_from_image(client, message):
    if not OCR_AVAILABLE or not message.media:
        return ""
    try:
        image_bytes = await client.download_media(message, bytes)
        if not image_bytes:
            return ""
        img = Image.open(io.BytesIO(image_bytes))
        text = pytesseract.image_to_string(img, lang="kor+eng").strip()
        if text:
            print(f"\U0001f50d OCR extracted {len(text)} chars from image")
        return text
    except Exception as e:
        print(f"\u26a0\ufe0f OCR error: {e}")
        return ""

# ============================================================
# KIS API (v7.8: uses per-code market division)
# ============================================================
class KISApi:
    def __init__(self):
        self.access_token = None
        self.token_expires = 0
        self.session = None

    async def ensure_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )

    async def get_token(self):
        now = time.time()
        if self.access_token and now < self.token_expires:
            return self.access_token
        await self.ensure_session()
        body = {
            "grant_type": "client_credentials",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
        }
        try:
            async with self.session.post(f"{KIS_BASE_URL}/oauth2/tokenP", json=body) as resp:
                data = await resp.json()
            if "access_token" in data:
                self.access_token = data["access_token"]
                self.token_expires = now + 85000
                print("\U0001f511 KIS 토큰 발급 성공")
                return self.access_token
            print(f"\u274c KIS 토큰 실패: {data}")
            return None
        except Exception as e:
            print(f"\u274c KIS 토큰 에러: {e}")
            return None

    def _headers(self, tr_id):
        return {
            "authorization": f"Bearer {self.access_token}",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
            "tr_id": tr_id,
            "Content-Type": "application/json; charset=utf-8",
        }

    async def get_stock_price(self, stock_code, market_code="J"):
        """현재가 조회. market_code: 'J'=KOSPI, 'Q'=KOSDAQ."""
        token = await self.get_token()
        if not token:
            return None
        await self.ensure_session()
        params = {"FID_COND_MRKT_DIV_CODE": market_code, "FID_INPUT_ISCD": stock_code}
        try:
            async with self.session.get(
                f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price",
                headers=self._headers("FHKST01010100"), params=params
            ) as resp:
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
            print(f"\u26a0\ufe0f 시세실패 [{stock_code}]: {data.get('msg1', '')}")
            return None
        except asyncio.TimeoutError:
            print(f"\u26a0\ufe0f 시세 타임아웃 [{stock_code}]")
            return None
        except Exception as e:
            print(f"\u274c 시세에러 [{stock_code}]: {e}")
            return None

    async def get_rolling_volume(self, stock_code, market_code="J", minutes=20):
        """최근 N분 rolling 거래대금 (validates candle timestamps)."""
        token = await self.get_token()
        if not token:
            return 0
        await self.ensure_session()
        now = datetime.now()
        params = {
            "FID_ETC_CLS_CODE": "",
            "FID_COND_MRKT_DIV_CODE": market_code,
            "FID_INPUT_ISCD": stock_code,
            "FID_INPUT_HOUR_1": now.strftime("%H%M%S"),
            "FID_PW_DATA_INCU_YN": "N",
        }
        try:
            async with self.session.get(
                f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
                headers=self._headers("FHKST03010200"), params=params
            ) as resp:
                data = await resp.json()
            if data.get("rt_cd") == "0":
                items = data.get("output2", [])
                cutoff = now - timedelta(minutes=minutes)
                total = 0
                for item in items:
                    ts = item.get("stck_cntg_hour", "")
                    if ts and len(ts) >= 4:
                        try:
                            candle_dt = now.replace(
                                hour=int(ts[:2]), minute=int(ts[2:4]),
                                second=0, microsecond=0
                            )
                            if candle_dt < cutoff:
                                break
                        except (ValueError, IndexError):
                            pass
                    total += int(item.get("cntg_vol", "0")) * int(item.get("stck_prpr", "0"))
                return total
            print(f"\u26a0\ufe0f 분봉실패 [{stock_code}]: {data.get('msg1', '')}")
            return 0
        except asyncio.TimeoutError:
            print(f"\u26a0\ufe0f 분봉 타임아웃 [{stock_code}]")
            return 0
        except Exception as e:
            print(f"\u274c 분봉에러 [{stock_code}]: {e}")
            return 0

    async def get_daily_prices(self, stock_code, market_code="J", count=60):
        """일별 종가 조회 (validates reverse-chronological order)."""
        token = await self.get_token()
        if not token:
            return []
        await self.ensure_session()
        today = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")
        params = {
            "FID_COND_MRKT_DIV_CODE": market_code,
            "FID_INPUT_ISCD": stock_code,
            "FID_INPUT_DATE_1": start_date,
            "FID_INPUT_DATE_2": today,
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": "0",
        }
        try:
            async with self.session.get(
                f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
                headers=self._headers("FHKST03010100"), params=params
            ) as resp:
                data = await resp.json()
            if data.get("rt_cd") == "0":
                items = data.get("output2", [])
                dated = []
                for item in items[:count]:
                    close = int(item.get("stck_clpr", "0"))
                    date_str = item.get("stck_bsop_date", "")
                    if close > 0 and date_str:
                        dated.append((date_str, close))
                dated.sort(key=lambda x: x[0], reverse=True)
                return [close for _, close in dated]
            print(f"\u26a0\ufe0f 일봉실패 [{stock_code}]: {data.get('msg1', '')}")
            return []
        except asyncio.TimeoutError:
            print(f"\u26a0\ufe0f 일봉 타임아웃 [{stock_code}]")
            return []
        except Exception as e:
            print(f"\u274c 일봉에러 [{stock_code}]: {e}")
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
        position = (
            (price - w52_low) / (w52_high - w52_low) * 100
            if w52_high > w52_low > 0 else 50
        )
        risk_score = 0
        if change_rate >= 10:
            risk_score += 3
        elif change_rate >= 5:
            risk_score += 2
        elif change_rate >= 3:
            risk_score += 1
        if position >= 90:
            risk_score += 2
        elif position >= 75:
            risk_score += 1
        if position <= 10:
            risk_score += 2
        elif position <= 25:
            risk_score += 1
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
            return "데이터 부족", "\u2753"
        ma5 = sum(closes[:5]) / 5
        ma20 = sum(closes[:20]) / 20 if len(closes) >= 20 else None
        ma60 = sum(closes[:60]) / 60 if len(closes) >= 60 else None
        price = current_price if current_price else closes[0]
        parts = []
        signals = {"bullish": 0, "bearish": 0}

        if price > ma5:
            parts.append(f"5일선({ma5:,.0f}) 위")
            signals["bullish"] += 1
        else:
            parts.append(f"5일선({ma5:,.0f}) 아래")
            signals["bearish"] += 1

        if ma20:
            if price > ma20:
                parts.append(f"20일선({ma20:,.0f}) 위")
                signals["bullish"] += 1
            else:
                parts.append(f"20일선({ma20:,.0f}) 아래")
                signals["bearish"] += 1

        if ma60:
            if price > ma60:
                parts.append(f"60일선({ma60:,.0f}) 위")
                signals["bullish"] += 1
            else:
                parts.append(f"60일선({ma60:,.0f}) 아래")
                signals["bearish"] += 1

        cross = ""
        if ma20 and len(closes) >= 21:
            prev_ma5 = sum(closes[1:6]) / 5
            prev_ma20 = sum(closes[1:21]) / 20
            if prev_ma5 <= prev_ma20 and ma5 > ma20:
                cross = "\U0001f31f 골든크로스(5/20)"
                signals["bullish"] += 2
            elif prev_ma5 >= prev_ma20 and ma5 < ma20:
                cross = "\U0001f480 데드크로스(5/20)"
                signals["bearish"] += 2

        b, r = signals["bullish"], signals["bearish"]
        if b >= 4:
            state = "\U0001f7e2 강세 (Strong Bullish)"
        elif b > r:
            state = "\U0001f7e2 상승추세 (Bullish)"
        elif r >= 4:
            state = "\U0001f534 약세 (Strong Bearish)"
        elif r > b:
            state = "\U0001f534 하락추세 (Bearish)"
        else:
            state = "\U0001f7e1 횡보 (Neutral)"

        ma_detail = " | ".join(parts)
        if cross:
            ma_detail += f" | {cross}"
        return state, ma_detail
    except Exception:
        return "\u2753 계산 불가", ""

# ============================================================
# HELPER FUNCTIONS
# ============================================================
def contains_crypto_keyword(text):
    if not text:
        return False
    lower = text.lower()
    return any(kw in lower for kw in _CRYPTO_LOWER)


def contains_earnings_keyword(text):
    """Two-layer filter with high-confidence bypass."""
    if not text:
        return False
    lower = text.lower()
    has_anchor = any(kw in lower for kw in _ANCHOR_LOWER)
    has_data = any(kw in lower for kw in _DATA_LOWER)
    if has_anchor and has_data:
        return True
    has_high_conf = any(kw in lower for kw in _HIGH_CONF_LOWER)
    if has_high_conf and FINANCIAL_NUMBER_PATTERN.search(text):
        return True
    return False

# ============================================================
# SIMHASH DUPLICATE DETECTOR
# ============================================================
def _simhash(text, hashbits=64):
    cleaned = " ".join(text.lower().split())
    if len(cleaned) < 3:
        return int(hashlib.md5(cleaned.encode()).hexdigest(), 16) % (2 ** hashbits)
    shingles = [cleaned[i:i+3] for i in range(len(cleaned) - 2)]
    v = [0] * hashbits
    for shingle in shingles:
        h = int(hashlib.md5(shingle.encode()).hexdigest(), 16)
        for i in range(hashbits):
            v[i] += 1 if h & (1 << i) else -1
    fingerprint = 0
    for i in range(hashbits):
        if v[i] > 0:
            fingerprint |= (1 << i)
    return fingerprint


def _hamming_distance(a, b):
    x = a ^ b
    count = 0
    while x:
        count += 1
        x &= x - 1
    return count


class DuplicateDetector:
    """Scalable dedup: exact MD5 + simhash with Hamming distance. Coroutine-safe."""

    def __init__(self, threshold=0.85, max_history=500, ttl_hours=6):
        self.max_history = max_history
        self.ttl_seconds = ttl_hours * 3600
        self.hamming_threshold = int((1 - threshold) * 64)
        self.seen_hashes = {}
        self.seen_simhashes = []
        self.stats = {"total": 0, "unique": 0, "duplicate": 0, "skipped": 0}
        self._lock = asyncio.Lock()

    def _clean_old(self):
        now = time.time()
        old_count = len(self.seen_hashes)
        self.seen_hashes = {h: t for h, t in self.seen_hashes.items() if now - t < self.ttl_seconds}
        self.seen_simhashes = [(sh, t) for sh, t in self.seen_simhashes if now - t < self.ttl_seconds]
        if len(self.seen_simhashes) > self.max_history:
            self.seen_simhashes = self.seen_simhashes[-self.max_history:]
        cleaned = old_count - len(self.seen_hashes)
        if cleaned:
            print(f"\U0001f9f9 Cleaned {cleaned} old entries")

    async def is_duplicate(self, text):
        async with self._lock:
            self.stats["total"] += 1
            if not text or not text.strip():
                self.stats["skipped"] += 1
                return True
            self._clean_old()
            now = time.time()
            text_hash = hashlib.md5(" ".join(text.lower().split()).encode()).hexdigest()
            if text_hash in self.seen_hashes:
                self.stats["duplicate"] += 1
                return True
            if len(text.strip()) > 15:
                text_simhash = _simhash(text)
                for old_simhash, _ in self.seen_simhashes:
                    if _hamming_distance(text_simhash, old_simhash) <= self.hamming_threshold:
                        self.stats["duplicate"] += 1
                        return True
                self.seen_simhashes.append((text_simhash, now))
            self.seen_hashes[text_hash] = now
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
    start_time = time.time()
    print("=" * 50)
    print(" Telegram Monitor v7.8")
    print(" BOT1: Filter+Dedup (no crypto) -> @my_filtered_news")
    print(" BOT2: 실적/공시 -> @jason_earnings")
    print(" BOT3: 종목별 시세/거래대금/RISK/MA + OCR -> @alerts_forme")
    print("=" * 50)

    if not all([API_ID, API_HASH, SESSION_STRING, TARGET_CHANNEL]):
        missing = [v for v, val in [
            ("API_ID", API_ID), ("API_HASH", API_HASH),
            ("SESSION_STRING", SESSION_STRING), ("TARGET_CHANNEL", TARGET_CHANNEL),
        ] if not val]
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

    print("\U0001f4e5 Loading stock universe from KIS master files...")
    await stock_universe.load()
    if not stock_universe.all_codes:
        print("\u26a0\ufe0f Stock universe empty - falling back without name matching")

    client = TelegramClient(
        StringSession(SESSION_STRING), API_ID, API_HASH,
        connection_retries=5, retry_delay=2,
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
        """Refresh monitored chats and prune stale ones."""
        current_ids = set()
        added = 0
        async for dialog in client.iter_dialogs():
            entity = dialog.entity
            try:
                peer_id = utils.get_peer_id(entity)
            except Exception:
                continue
            if peer_id in exclude_ids:
                continue
            is_channel = isinstance(entity, Channel) and entity.broadcast
            is_megagroup = isinstance(entity, Channel) and entity.megagroup
            is_chat = isinstance(entity, Chat)
            if is_channel or is_megagroup or is_chat:
                current_ids.add(peer_id)
                if peer_id not in monitored_ids:
                    monitored_ids.add(peer_id)
                    label = "\U0001f4fa Channel" if is_channel else "\U0001f465 Group"
                    print(f"  {label}: {entity.title} (id: {peer_id})")
                    added += 1
        stale = monitored_ids - current_ids - exclude_ids
        if stale:
            monitored_ids.difference_update(stale)
            print(f"  \U0001f9f9 Pruned {len(stale)} stale chats")
        return added

    await refresh_dialogs()
    print(f"\U0001f4ca Monitoring {len(monitored_ids)} chats")
    print(f"\U0001f4ec All unique (no crypto) -> {TARGET_CHANNEL}")
    if EARNINGS_CHANNEL:
        print(f"\U0001f4c8 실적/공시 -> {EARNINGS_CHANNEL}")
    if kis:
        print(f"\U0001f6a8 종목 알림 (시세/거래대금/RISK/MA) -> {VOLUME_ALERT_CHANNEL}")
    if OCR_AVAILABLE:
        print("\U0001f50d OCR enabled: image stock detection active")

    bg_tasks = []
    shutdown_event = asyncio.Event()

    def _signal_handler():
        print("\U0001f6d1 Received shutdown signal...")
        shutdown_event.set()
        client.disconnect()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass

    async def heartbeat():
        try:
            while not shutdown_event.is_set():
                await asyncio.sleep(600)
                stats = detector.get_stats()
                up = int(time.time() - start_time)
                print(
                    f"\U0001f49a Heartbeat: uptime={up//3600}h{(up%3600)//60}m | "
                    f"connected={client.is_connected()} | "
                    f"chats={len(monitored_ids)} | "
                    f"total={stats['total']} unique={stats['unique']} "
                    f"dup={stats['duplicate']} skip={stats['skipped']}"
                )
        except asyncio.CancelledError:
            pass

    bg_tasks.append(asyncio.create_task(heartbeat()))

    async def periodic_refresh():
        try:
            while not shutdown_event.is_set():
                await asyncio.sleep(1800)
                print("\U0001f504 Refreshing dialog list...")
                added = await refresh_dialogs()
                print(f"  \u2705 {added} new, total: {len(monitored_ids)}")
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

            # ====== OCR ======
            ocr_text = ""
            if has_media and OCR_AVAILABLE and kis:
                ocr_text = await extract_text_from_image(client, event.message)

            combined_text = f"{msg}\n{ocr_text}".strip() if ocr_text else msg

            # ====== BOT 1: Unique messages (no crypto) ======
            if not msg and not ocr_text:
                if has_media:
                    try:
                        await safe_forward(client, TARGET_CHANNEL, event.message)
                    except Exception:
                        try:
                            icon = "\U0001f4fa" if isinstance(chat, Channel) and chat.broadcast else "\U0001f465"
                            await safe_send(client, TARGET_CHANNEL, f"\U0001f4ce {icon}**{chat_name}** [미디어 메시지]", link_preview=False)
                        except Exception:
                            pass
                return

            if contains_crypto_keyword(combined_text):
                return

            if await detector.is_duplicate(combined_text):
                return

            print(f"\U0001f4e8 [{chat_name}] Unique msg ({len(combined_text)} chars)")
            forwarded = False
            try:
                await safe_forward(client, TARGET_CHANNEL, event.message)
                forwarded = True
                print(f"  \u2705 Forwarded to {TARGET_CHANNEL}")
            except Exception:
                try:
                    icon = "\U0001f4fa" if isinstance(chat, Channel) and chat.broadcast else "\U0001f465"
                    await safe_send(client, TARGET_CHANNEL, f"{icon}**{chat_name}**\n{combined_text[:500]}", link_preview=False)
                    forwarded = True
                except Exception:
                    pass
            if not forwarded:
                print(f"  \u274c FAILED to deliver to {TARGET_CHANNEL}!")

            # ====== BOT 2: 실적/공시 ======
            if EARNINGS_CHANNEL and combined_text and contains_earnings_keyword(combined_text):
                matched = [kw for kw in _ALL_EARNINGS_LOWER if kw in combined_text.lower()][:5]
                print(f"  \U0001f4c8 실적/공시: {matched}")
                try:
                    await safe_forward(client, EARNINGS_CHANNEL, event.message)
                    print(f"  \u2705 -> {EARNINGS_CHANNEL}")
                except Exception:
                    try:
                        await safe_send(
                            client, EARNINGS_CHANNEL,
                            f"\U0001f4c8 **[실적/공시]** {chat_name}\n\U0001f511 {', '.join(matched)}\n{combined_text[:500]}",
                            link_preview=False,
                        )
                    except Exception:
                        pass

            # ====== BOT 3: 종목 알림 (v7.8: correct market code per stock) ======
            if kis and combined_text:
                codes = stock_universe.find_stocks_in_text(combined_text)
                text_codes = set(stock_universe.find_stocks_in_text(msg)) if msg else set()

                for code in codes:
                    if not cooldown.can_alert(code):
                        continue

                    # v7.8: look up correct market code for this stock
                    mkt = stock_universe.lookup_market(code)

                    price_info = await kis.get_stock_price(code, market_code=mkt)
                    if not price_info:
                        cooldown.reset(code)
                        continue

                    if price_info.get("acml_tr_pbmn", 0) < 5_000_000_000:
                        cooldown.reset(code)
                        continue

                    # v7.8: name comes directly from correctly parsed universe
                    # (no more ISIN stripping needed)
                    api_name = (price_info.get("name") or "").strip()
                    universe_name = stock_universe.lookup_name(code) or ""
                    name = universe_name or api_name or code

                    rolling_vol = await kis.get_rolling_volume(code, market_code=mkt, minutes=20)
                    daily_closes = await kis.get_daily_prices(code, market_code=mkt, count=60)
                    risk_label, risk_emoji = compute_risk_level(price_info)
                    ma_state, ma_detail = compute_ma_state(daily_closes, price_info["price"])

                    change_val = float(price_info.get("change_rate", "0"))
                    direction = "\U0001f53a" if change_val > 0 else ("\U0001f53b" if change_val < 0 else "\u25ab")

                    from_ocr = ocr_text and code not in text_codes
                    source_tag = " [OCR]" if from_ocr else ""
                    mkt_label = "KOSDAQ" if mkt == "Q" else "KOSPI"
                    print(
                        f"  \U0001f4b0{source_tag} [{code}/{mkt_label}] {name} {price_info['price']:,}원 "
                        f"({change_val:+.2f}%) 20분거래대금: {rolling_vol/100_000_000:.2f}억"
                    )

                    alert_lines = [
                        f"\U0001f514 **종목 알림 ({name})**",
                        "",
                        f"\U0001f4cc **{name}** ({code} | {mkt_label})",
                        f"\U0001f4b0 현재가: {price_info['price']:,}원 {direction} {change_val:+.2f}%",
                        f"\U0001f4c8 시가: {price_info.get('open_price', 0):,} | "
                        f"고가: {price_info.get('high_price', 0):,} | "
                        f"저가: {price_info.get('low_price', 0):,}",
                        f"\U0001f4ca 거래량: {price_info.get('acml_vol', 0):,}주",
                        f"\U0001f4b5 누적거래대금: {price_info.get('acml_tr_pbmn', 0)/100_000_000:.1f}억",
                        f"\U0001f4b5 20분 거래대금: {rolling_vol/100_000_000:.1f}억",
                        "",
                        f"\u26a0\ufe0f RISK: {risk_label}",
                        f"\U0001f4c9 이동평균: {ma_state}",
                    ]
                    if ma_detail:
                        alert_lines.append(f"  \u27a1 {ma_detail}")

                    w52h = price_info.get("w52_hgpr", 0)
                    w52l = price_info.get("w52_lwpr", 0)
                    if w52h and w52l:
                        pos_pct = (
                            (price_info["price"] - w52l) / (w52h - w52l) * 100
                            if w52h > w52l else 0
                        )
                        alert_lines.append(f"\U0001f4cd 52주: {w52l:,} ~ {w52h:,} (현재 {pos_pct:.0f}% 위치)")

                    ocr_label = " \U0001f50d(OCR)" if from_ocr else ""
                    alert_lines.extend([
                        "",
                        f"\U0001f4ac 출처: {chat_name}{ocr_label}",
                        f"\u23f0 {datetime.now().strftime('%H:%M:%S')}",
                    ])

                    try:
                        await safe_send(client, VOLUME_ALERT_CHANNEL, "\n".join(alert_lines), link_preview=False)
                        print(f"  \U0001f6a8 ALERT: {name} ({code}/{mkt_label}) \u2705")
                    except Exception as e:
                        print(f"  \u274c Alert failed: {e}")
                    await asyncio.sleep(0.3)

        except errors.FloodWaitError as e:
            await asyncio.sleep(e.seconds + 1)
        except Exception as e:
            print(f"  \u274c Handler error: {e}")
            import traceback
            traceback.print_exc()

    print(f"\U0001f3a7 Listening... (v7.8)")
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
