"""
Telegram Channel & Group Monitor v8.3
======================================
BOT 1: All unique messages (no crypto/coin) -> TARGET_CHANNEL (@my_filtered_news)
BOT 2: 실적/공시 keyword messages -> EARNINGS_CHANNEL (@jason_earnings)
BOT 3: 종목 언급 시 -> 현재가, 거래대금, RISK, 이동평균 상태 알림 -> VOLUME_ALERT_CHANNEL (@alerts_forme)

v8.3 Changes:
  [BUG FIX] BOT 1 message loss: BOT 3 stock alert processing now runs as background
            task (asyncio.create_task) instead of blocking the message handler.
            Previously, multiple KIS API calls (3 per stock × N stocks) blocked the
            handler for seconds, causing Telegram to skip incoming messages.
  [BUG FIX] KeyError crash: price_info["price"] replaced with safe .get() access
  [BUG FIX] Cooldown logic: can_alert() no longer consumes cooldown prematurely;
            mark_alerted() only called after successful alert delivery
  [BUG FIX] Float conversion: change_rate "N/A" or non-numeric no longer crashes
            (new _safe_float helper)
  [BUG FIX] VOLUME_THRESHOLD env var now actually used (was hardcoded 5B)
  [IMPROVE] HTTP status validation before JSON parsing in all KIS API methods
  [IMPROVE] JSON parse errors caught specifically (ContentTypeError/ValueError)
  [IMPROVE] Price validation: negative/zero prices return N/A in risk calculation,
            52-week position clamped to 0-100%
  [IMPROVE] OCR silent failure logging for photos that return empty text
  [IMPROVE] Stock detection logging shows which codes were found per message
  [IMPROVE] Configurable API_TIMEOUT env var (default 30s, was hardcoded)
  [IMPROVE] Crypto filter now logs which keyword triggered (debug false positives)
  [IMPROVE] get_peer_id failures now logged with chat name

v8.2 Changes:
  [BUG FIX] BOT 1: Whitespace-only msg (space/newline/tab) with media was silently
            dropped — msg.strip() now applied early so media-only branch handles it
  [BUG FIX] BOT 1: Short crypto keywords (eth, ada, dot, pepe, etc.) were substring-
            matching inside normal words (method, adapted, dotted) — legitimate
            stock messages silently filtered. Now uses word-boundary regex for
            short keywords, substring matching for long/Korean keywords.
  [BUG FIX] OCR downloaded ALL media types (videos/audio/stickers) before failing
            at Image.open() — now only processes photos and image documents.
            Large video downloads were blocking the handler for minutes, causing
            message backlog and perceived message loss.
  [BUG FIX] Version string mismatch (main printed v8.0, docstring said v8.1)
  [IMPROVE] BOT 1: Added logging for crypto-filtered and dedup-dropped messages
            (was silent — impossible to debug missing messages)
  [IMPROVE] DuplicateDetector: Logs SimHash hamming distance on near-match hits
            to help identify false positives
  [IMPROVE] FloodWaitError handler now logs lost message preview before sleeping

v8.1 Changes:
  [FEATURE] "WATCH" header highlight when all conditions met:
            MA Bullish + 52주 below 60% + 거래대금 > 100억 + RISK Yellow/Green

v8.0 Changes:
  [BUG FIX] Signal handler now properly schedules async disconnect (was sync call)
  [BUG FIX] KIS API int() conversions now validate data before parsing (crash prevention)
  [BUG FIX] safe_forward/safe_send return False on final retry instead of raising
  [BUG FIX] AlertCooldown now cleans old entries periodically (memory leak fix)
  [BUG FIX] find_stocks_in_text returns deduplicated set (was list with possible dupes)
  [BUG FIX] Empty/None chat titles now default to "Unknown"
  [IMPROVE] OCR error logging now includes chat name context
  [IMPROVE] Master file retry uses exponential backoff (60s, 120s, 240s)
  [IMPROVE] Removed dead code: unused _sorted_names in StockUniverse
  [IMPROVE] _hamming_distance uses Python 3.10+ int.bit_count() when available

v7.9 Changes:
  [BUG FIX] KIS token expiry now uses API response expires_in (was hardcoded 85000s)
  [BUG FIX] aiohttp session auto-recovery on zombie/error state
  [BUG FIX] OCR now runs independently of KIS config (benefits BOT 1 & 2)
  [BUG FIX] Master file download retries on failure (3 attempts, 60s interval)
  [PERF] Stock name search uses length-bucketed index (avoid full scan)
  [PERF] DuplicateDetector._clean_old runs periodically (every 60s, not every call)
  [PERF] KIS API rate limiter via asyncio.Semaphore (max 10 concurrent)
  [PERF] Keyword matching uses single-pass set intersection
  [PERF] Reduced memory: reuse compiled regex, __slots__ on hot classes

v7.8 Changes:
  - Fixed master file parsing: now uses official KIS structure
  - Fixed KIS API market code: KOSDAQ stocks now queried with "Q" instead of "J"
  - Handles duplicate stock names across KOSPI/KOSDAQ by appending market suffix

v7.7 Changes:
  - Fixed BOT 1: media-only messages now checked via OCR against crypto filter & dedup
  - Fixed dedup to check combined_text (msg + OCR) not just msg
  - Fixed BOT 3: pre-compute text_stock_codes outside per-code loop
  - Pinned all dependency versions in requirements.txt
  - Added SIGTERM signal handling for graceful Docker/Heroku shutdown
  - Dockerfile runs as non-root user
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
from telethon.types import Channel, Chat, MessageMediaPhoto, MessageMediaDocument

# OCR imports (optional)
try:
    from PIL import Image
    import pytesseract
    OCR_AVAILABLE = True
    for tp in ("/app/.apt/usr/bin/tesseract", "/usr/bin/tesseract", "/usr/local/bin/tesseract"):
        if os.path.exists(tp):
            pytesseract.pytesseract.tesseract_cmd = tp
            break
    print("✅ OCR available (Tesseract)")
except ImportError:
    OCR_AVAILABLE = False
    print("⚠️ OCR not available (install Pillow + pytesseract)")

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
API_TIMEOUT = int(os.environ.get("API_TIMEOUT", "30"))  # seconds, configurable

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
# v8.2: Split short English keywords for word-boundary matching (prevent false positives)
# Short keywords (≤4 chars, ASCII) like "eth" match inside "method" with substring search
_CRYPTO_SHORT = frozenset(k for k in _CRYPTO_LOWER if len(k) <= 4 and k.isascii())
_CRYPTO_LONG = _CRYPTO_LOWER - _CRYPTO_SHORT
_CRYPTO_SHORT_PATTERN = re.compile(
    r'\b(' + '|'.join(re.escape(k) for k in sorted(_CRYPTO_SHORT, key=len, reverse=True)) + r')\b',
    re.IGNORECASE | re.ASCII  # v8.2: ASCII mode so Korean chars = non-word boundary
) if _CRYPTO_SHORT else None
_ANCHOR_LOWER = frozenset(k.lower() for k in EARNINGS_ANCHOR_KEYWORDS)
_DATA_LOWER = frozenset(k.lower() for k in EARNINGS_DATA_KEYWORDS)
_HIGH_CONF_LOWER = frozenset(k.lower() for k in HIGH_CONFIDENCE_EARNINGS_KEYWORDS)
_ALL_EARNINGS_LOWER = _ANCHOR_LOWER | _DATA_LOWER | _HIGH_CONF_LOWER

FINANCIAL_NUMBER_PATTERN = re.compile(r'[\d,]+\s*(억|조|원|백만|천만)')
STOCK_CODE_PATTERN = re.compile(r'\b(\d{6})\b')

# ============================================================
# DYNAMIC STOCK UNIVERSE (v7.9: length-bucketed name index)
# ============================================================
class StockUniverse:
    """
    Downloads KIS master files to build name<->code mappings.
    v7.9: Uses length-bucketed index for O(N/bucket) name search
    instead of O(N) full scan. Also stores market type per code.
    """

    __slots__ = (
        'name_to_code', 'code_to_name', 'code_to_market',
        'all_codes', 'last_refresh', 'refresh_interval',
        '_name_buckets',
    )

    def __init__(self):
        self.name_to_code = {}
        self.code_to_name = {}
        self.code_to_market = {}
        self.all_codes = set()
        self.last_refresh = 0
        self.refresh_interval = 86400
        self._name_buckets = {}  # v7.9: {length: [names...]} for faster search

    def _parse_master_file(self, raw_bytes, tail_len, market_code):
        """Parse a single KIS master file (KOSPI or KOSDAQ)."""
        name_to_code = {}
        code_to_name = {}
        code_to_market = {}
        text = raw_bytes.decode("cp949", errors="ignore")
        for row in text.strip().split("\n"):
            if not row or len(row) <= tail_len + 21:
                continue
            front = row[:len(row) - tail_len]
            short_code_raw = front[0:9].strip()
            code_match = re.search(r'(\d{6})', short_code_raw)
            if not code_match:
                continue
            code = code_match.group(1)
            name = front[21:].strip()
            if not name:
                continue
            name_to_code[name] = code
            code_to_name[code] = name
            code_to_market[code] = market_code
        return name_to_code, code_to_name, code_to_market

    def _build_index(self):
        """v7.9: Build length-bucketed name index for faster text search.
        v8.0: Removed unused _sorted_names."""
        self._name_buckets = {}
        for name in self.name_to_code:
            if len(name) < 2:
                continue
            ln = len(name)
            if ln not in self._name_buckets:
                self._name_buckets[ln] = []
            self._name_buckets[ln].append(name)

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
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=API_TIMEOUT)) as resp:
                        if resp.status != 200:
                            print(f"⚠️ Failed to download {market} master: HTTP {resp.status}")
                            continue
                        data = await resp.read()
                    with zipfile.ZipFile(io.BytesIO(data)) as zf:
                        for filename in zf.namelist():
                            raw = zf.read(filename)
                            n2c, c2n, c2m = self._parse_master_file(raw, tail_len, market_code)
                            for name, code in n2c.items():
                                if name in all_name_to_code and all_name_to_code[name] != code:
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
                    print(f"✅ {market} master loaded: {len(c2n)} stocks")
                except Exception as e:
                    print(f"❌ Error loading {market} master: {e}")
            if all_name_to_code:
                self.name_to_code = all_name_to_code
                self.code_to_name = all_code_to_name
                self.code_to_market = all_code_to_market
                self.all_codes = set(all_code_to_name.keys())
                self._build_index()  # v7.9: build bucketed index
                self.last_refresh = time.time()
                print(f"📊 Stock universe: {len(self.name_to_code)} names, {len(self.all_codes)} codes")
            else:
                print("⚠️ Stock universe empty - master files may have changed format")
        finally:
            if close_session:
                await session.close()

    async def load_with_retry(self, session=None, max_retries=3, retry_delay=60):
        """v7.9: Retry master file download on failure.
        v8.0: Exponential backoff (60s, 120s, 240s...)."""
        for attempt in range(max_retries):
            await self.load(session)
            if self.all_codes:
                return
            if attempt < max_retries - 1:
                delay = retry_delay * (2 ** attempt)  # v8.0: exponential backoff
                print(f"🔄 Master file retry {attempt + 2}/{max_retries} in {delay}s...")
                await asyncio.sleep(delay)
        print("⚠️ All master file download attempts failed")

    async def ensure_fresh(self, session=None):
        if time.time() - self.last_refresh > self.refresh_interval:
            print("🔄 Refreshing stock universe...")
            await self.load_with_retry(session)

    def lookup_code(self, name):
        return self.name_to_code.get(name)

    def lookup_name(self, code):
        return self.code_to_name.get(code)

    def lookup_market(self, code):
        return self.code_to_market.get(code, "J")

    def find_stocks_in_text(self, text):
        """v7.9: Optimized with early-exit on short text + bucketed name scan."""
        if not text:
            return []
        found = set()
        # 6-digit code matching (fast regex)
        for m in STOCK_CODE_PATTERN.finditer(text):
            code = m.group()
            if code != "000000" and code in self.all_codes:
                found.add(code)
        # Name matching: only check names that could fit in text
        text_len = len(text)
        text_upper = text.upper()
        for name_len, names in self._name_buckets.items():
            if name_len > text_len:
                continue  # v7.9: skip names longer than text
            for name in names:
                if name in text or name.upper() in text_upper:
                    code = self.name_to_code.get(name)
                    if code:
                        found.add(code)
        return list(found)

stock_universe = StockUniverse()

# ============================================================
# OCR
# ============================================================
async def extract_text_from_image(client, message, chat_name=""):
    """v8.0: Added chat_name param for better error logging.
    v8.2: Only downloads photos and image documents (skip videos/audio/stickers)."""
    if not OCR_AVAILABLE or not message.media:
        return ""
    # v8.2: Only attempt OCR on photos and image-type documents
    media = message.media
    is_photo = isinstance(media, MessageMediaPhoto)
    is_image_doc = False
    if isinstance(media, MessageMediaDocument) and media.document:
        mime = getattr(media.document, 'mime_type', '') or ''
        is_image_doc = mime.startswith('image/')
    if not is_photo and not is_image_doc:
        return ""
    try:
        image_bytes = await client.download_media(message, bytes)
        if not image_bytes:
            return ""
        img = Image.open(io.BytesIO(image_bytes))
        text = pytesseract.image_to_string(img, lang="kor+eng").strip()
        if text:
            print(f"🔍 OCR extracted {len(text)} chars from image")
        return text
    except Exception as e:
        ctx = f" [{chat_name}]" if chat_name else ""
        print(f"⚠️ OCR error{ctx} (msg_id={message.id}): {e}")
        return ""

# ============================================================
# KIS API (v7.9: dynamic token expiry, session recovery, rate limit)
# ============================================================
class KISApi:
    __slots__ = ('access_token', 'token_expires', 'session', '_semaphore')

    def __init__(self):
        self.access_token = None
        self.token_expires = 0
        self.session = None
        self._semaphore = asyncio.Semaphore(10)  # v7.9: max 10 concurrent API calls

    async def _new_session(self):
        """v7.9: Create a fresh aiohttp session."""
        if self.session and not self.session.closed:
            try:
                await self.session.close()
            except Exception:
                pass
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=API_TIMEOUT)
        )

    async def ensure_session(self):
        """v7.9: Recover from zombie sessions."""
        if self.session is None or self.session.closed:
            await self._new_session()

    async def _safe_request(self, method, url, **kwargs):
        """v7.9: Wrapper with auto session recovery on connection errors."""
        await self.ensure_session()
        try:
            if method == "GET":
                return await self.session.get(url, **kwargs)
            else:
                return await self.session.post(url, **kwargs)
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
            # v7.9: Session might be broken, recreate and retry once
            print(f"⚠️ Session error ({e}), recreating...")
            await self._new_session()
            if method == "GET":
                return await self.session.get(url, **kwargs)
            else:
                return await self.session.post(url, **kwargs)

    async def get_token(self):
        now = time.time()
        if self.access_token and now < self.token_expires:
            return self.access_token
        body = {
            "grant_type": "client_credentials",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
        }
        try:
            async with await self._safe_request("POST", f"{KIS_BASE_URL}/oauth2/tokenP", json=body) as resp:
                if resp.status != 200:
                    print(f"❌ KIS 토큰 HTTP {resp.status}")
                    return None
                try:
                    data = await resp.json()
                except (aiohttp.ContentTypeError, ValueError) as e:
                    print(f"❌ KIS 토큰 JSON 파싱 실패: {e}")
                    return None
            if "access_token" in data:
                self.access_token = data["access_token"]
                # v7.9: Use actual expiry from API response (fallback 85000s)
                expires_in = int(data.get("expires_in", 85000))
                self.token_expires = now + expires_in - 300  # 5min safety margin
                print(f"🔑 KIS 토큰 발급 성공 (만료: {expires_in // 3600}h)")
                return self.access_token
            print(f"❌ KIS 토큰 실패: {data}")
            return None
        except Exception as e:
            print(f"❌ KIS 토큰 에러: {e}")
            return None

    def _headers(self, tr_id):
        return {
            "authorization": f"Bearer {self.access_token}",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
            "tr_id": tr_id,
            "Content-Type": "application/json; charset=utf-8",
        }

    @staticmethod
    def _safe_int(val, default=0):
        """v8.0: Safely convert API string to int without crashing."""
        if val is None:
            return default
        try:
            return int(str(val).replace(",", "").strip())
        except (ValueError, TypeError):
            return default

    async def get_stock_price(self, stock_code, market_code="J"):
        """현재가 조회. v7.9: rate-limited. v8.0: safe int parsing."""
        token = await self.get_token()
        if not token:
            return None
        params = {"FID_COND_MRKT_DIV_CODE": market_code, "FID_INPUT_ISCD": stock_code}
        async with self._semaphore:  # v7.9: rate limit
            try:
                async with await self._safe_request(
                    "GET",
                    f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price",
                    headers=self._headers("FHKST01010100"), params=params
                ) as resp:
                    if resp.status != 200:
                        print(f"⚠️ 시세 HTTP {resp.status} [{stock_code}]")
                        return None
                    try:
                        data = await resp.json()
                    except (aiohttp.ContentTypeError, ValueError) as e:
                        print(f"⚠️ 시세 JSON 파싱 실패 [{stock_code}]: {e}")
                        return None
                if data.get("rt_cd") == "0":
                    out = data.get("output", {})
                    if not out:
                        print(f"⚠️ 시세 응답 비어있음 [{stock_code}]")
                        return None
                    price = self._safe_int(out.get("stck_prpr"))
                    if price <= 0:
                        print(f"⚠️ 시세 price=0 [{stock_code}], skipping alert")
                        return None
                    return {
                        "name": out.get("hts_kor_isnm", stock_code),
                        "price": price,
                        "change_rate": out.get("prdy_ctrt", "0"),
                        "change_sign": out.get("prdy_vrss_sign", "3"),
                        "high_price": self._safe_int(out.get("stck_hgpr")),
                        "low_price": self._safe_int(out.get("stck_lwpr")),
                        "open_price": self._safe_int(out.get("stck_oprc")),
                        "prev_close": self._safe_int(out.get("stck_sdpr")),
                        "acml_tr_pbmn": self._safe_int(out.get("acml_tr_pbmn")),
                        "acml_vol": self._safe_int(out.get("acml_vol")),
                        "per": out.get("per", "N/A"),
                        "pbr": out.get("pbr", "N/A"),
                        "w52_hgpr": self._safe_int(out.get("w52_hgpr")),
                        "w52_lwpr": self._safe_int(out.get("w52_lwpr")),
                    }
                print(f"⚠️ 시세실패 [{stock_code}]: {data.get('msg1', '')}")
                return None
            except asyncio.TimeoutError:
                print(f"⚠️ 시세 타임아웃 [{stock_code}]")
                return None
            except Exception as e:
                print(f"❌ 시세에러 [{stock_code}]: {e}")
                return None

    async def get_rolling_volume(self, stock_code, market_code="J", minutes=20):
        """최근 N분 rolling 거래대금 (validates candle timestamps). v7.9: rate-limited."""
        token = await self.get_token()
        if not token:
            return 0
        now = datetime.now()
        params = {
            "FID_ETC_CLS_CODE": "",
            "FID_COND_MRKT_DIV_CODE": market_code,
            "FID_INPUT_ISCD": stock_code,
            "FID_INPUT_HOUR_1": now.strftime("%H%M%S"),
            "FID_PW_DATA_INCU_YN": "N",
        }
        async with self._semaphore:  # v7.9: rate limit
            try:
                async with await self._safe_request(
                    "GET",
                    f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
                    headers=self._headers("FHKST03010200"), params=params
                ) as resp:
                    if resp.status != 200:
                        print(f"⚠️ 분봉 HTTP {resp.status} [{stock_code}]")
                        return 0
                    try:
                        data = await resp.json()
                    except (aiohttp.ContentTypeError, ValueError) as e:
                        print(f"⚠️ 분봉 JSON 파싱 실패 [{stock_code}]: {e}")
                        return 0
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
                        total += self._safe_int(item.get("cntg_vol")) * self._safe_int(item.get("stck_prpr"))
                    return total
                print(f"⚠️ 분봉실패 [{stock_code}]: {data.get('msg1', '')}")
                return 0
            except asyncio.TimeoutError:
                print(f"⚠️ 분봉 타임아웃 [{stock_code}]")
                return 0
            except Exception as e:
                print(f"❌ 분봉에러 [{stock_code}]: {e}")
                return 0

    async def get_daily_prices(self, stock_code, market_code="J", count=60):
        """일별 종가 조회 (validates reverse-chronological order). v7.9: rate-limited."""
        token = await self.get_token()
        if not token:
            return []
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
        async with self._semaphore:  # v7.9: rate limit
            try:
                async with await self._safe_request(
                    "GET",
                    f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
                    headers=self._headers("FHKST03010100"), params=params
                ) as resp:
                    if resp.status != 200:
                        print(f"⚠️ 일봉 HTTP {resp.status} [{stock_code}]")
                        return []
                    try:
                        data = await resp.json()
                    except (aiohttp.ContentTypeError, ValueError) as e:
                        print(f"⚠️ 일봉 JSON 파싱 실패 [{stock_code}]: {e}")
                        return []
                if data.get("rt_cd") == "0":
                    items = data.get("output2", [])
                    dated = []
                    for item in items[:count]:
                        close = self._safe_int(item.get("stck_clpr"))
                        date_str = item.get("stck_bsop_date", "")
                        if close > 0 and date_str:
                            dated.append((date_str, close))
                    dated.sort(key=lambda x: x[0], reverse=True)
                    return [close for _, close in dated]
                print(f"⚠️ 일봉실패 [{stock_code}]: {data.get('msg1', '')}")
                return []
            except asyncio.TimeoutError:
                print(f"⚠️ 일봉 타임아웃 [{stock_code}]")
                return []
            except Exception as e:
                print(f"❌ 일봉에러 [{stock_code}]: {e}")
                return []

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

# ============================================================
# RISK LEVEL & MOVING AVERAGE HELPERS
# ============================================================
def _safe_float(val, default=0.0):
    """Safely convert API string to float without crashing."""
    if val is None:
        return default
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return default


def compute_risk_level(price_info):
    try:
        change_rate = abs(_safe_float(price_info.get("change_rate", "0")))
        price = price_info.get("price", 0)
        if not isinstance(price, (int, float)) or price <= 0:
            return "❓ N/A", "❓"
        w52_high = price_info.get("w52_hgpr", 0)
        w52_low = price_info.get("w52_lwpr", 0)
        position = (
            max(0, min(100, (price - w52_low) / (w52_high - w52_low) * 100))
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
            return "🔴 HIGH", "🔴"
        elif risk_score >= 2:
            return "🟡 MEDIUM", "🟡"
        else:
            return "🟢 LOW", "🟢"
    except Exception:
        return "❓ N/A", "❓"


def compute_ma_state(closes, current_price=None):
    try:
        if len(closes) < 5:
            return "데이터 부족", "❓"
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
                cross = "🌟 골든크로스(5/20)"
                signals["bullish"] += 2
            elif prev_ma5 >= prev_ma20 and ma5 < ma20:
                cross = "💀 데드크로스(5/20)"
                signals["bearish"] += 2

        b, r = signals["bullish"], signals["bearish"]
        if b >= 4:
            state = "🟢 강세 (Strong Bullish)"
        elif b > r:
            state = "🟢 상승추세 (Bullish)"
        elif r >= 4:
            state = "🔴 약세 (Strong Bearish)"
        elif r > b:
            state = "🔴 하락추세 (Bearish)"
        else:
            state = "🟡 횡보 (Neutral)"

        ma_detail = " | ".join(parts)
        if cross:
            ma_detail += f" | {cross}"
        return state, ma_detail
    except Exception:
        return "❓ 계산 불가", ""

# ============================================================
# HELPER FUNCTIONS (v7.9: optimized keyword matching)
# ============================================================
def contains_crypto_keyword(text):
    """v8.2: Short English keywords (eth, ada, dot, etc.) use word-boundary regex
    to prevent false positives from words like 'method', 'adapted', 'dotted'."""
    if not text:
        return False
    lower = text.lower()
    # Long keywords + Korean keywords: safe to use substring matching
    if any(kw in lower for kw in _CRYPTO_LONG):
        return True
    # Short ASCII keywords: require word boundary (e.g. \beth\b won't match 'method')
    if _CRYPTO_SHORT_PATTERN and _CRYPTO_SHORT_PATTERN.search(lower):
        return True
    return False


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
# SIMHASH DUPLICATE DETECTOR (v7.9: periodic cleanup)
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
    """v8.0: Uses int.bit_count() on Python 3.10+, falls back to Kernighan's."""
    x = a ^ b
    if hasattr(x, 'bit_count'):
        return x.bit_count()
    count = 0
    while x:
        count += 1
        x &= x - 1
    return count


class DuplicateDetector:
    """Scalable dedup: exact MD5 + simhash with Hamming distance.
    v7.9: Periodic cleanup (every 60s) instead of per-call."""

    __slots__ = (
        'max_history', 'ttl_seconds', 'hamming_threshold',
        'seen_hashes', 'seen_simhashes', 'stats', '_lock',
        '_last_clean_time',
    )

    def __init__(self, threshold=0.85, max_history=500, ttl_hours=6):
        self.max_history = max_history
        self.ttl_seconds = ttl_hours * 3600
        self.hamming_threshold = int((1 - threshold) * 64)
        self.seen_hashes = {}
        self.seen_simhashes = []
        self.stats = {"total": 0, "unique": 0, "duplicate": 0, "skipped": 0}
        self._lock = asyncio.Lock()
        self._last_clean_time = time.time()  # v7.9

    def _clean_old(self):
        """v7.9: Only clean if 60s have passed since last clean."""
        now = time.time()
        if now - self._last_clean_time < 60:
            return  # v7.9: skip frequent cleanup
        self._last_clean_time = now
        old_count = len(self.seen_hashes)
        self.seen_hashes = {h: t for h, t in self.seen_hashes.items() if now - t < self.ttl_seconds}
        self.seen_simhashes = [(sh, t) for sh, t in self.seen_simhashes if now - t < self.ttl_seconds]
        if len(self.seen_simhashes) > self.max_history:
            self.seen_simhashes = self.seen_simhashes[-self.max_history:]
        cleaned = old_count - len(self.seen_hashes)
        if cleaned:
            print(f"🧹 Cleaned {cleaned} old entries")

    async def is_duplicate(self, text):
        async with self._lock:
            self.stats["total"] += 1
            if not text or not text.strip():
                self.stats["skipped"] += 1
                return True
            self._clean_old()
            now = time.time()
            normalized = " ".join(text.lower().split())
            text_hash = hashlib.md5(normalized.encode()).hexdigest()
            if text_hash in self.seen_hashes:
                self.stats["duplicate"] += 1
                return True
            if len(text.strip()) > 15:
                text_simhash = _simhash(text)
                for old_simhash, _ in self.seen_simhashes:
                    dist = _hamming_distance(text_simhash, old_simhash)
                    if dist <= self.hamming_threshold:
                        self.stats["duplicate"] += 1
                        # v8.2: log SimHash near-match distance for debugging false positives
                        print(f"    🔎 SimHash match (hamming={dist}/{self.hamming_threshold}): {text[:60]}...")
                        return True
                self.seen_simhashes.append((text_simhash, now))
            self.seen_hashes[text_hash] = now
            self.stats["unique"] += 1
            return False

    def get_stats(self):
        return self.stats


class AlertCooldown:
    """v8.0: Added periodic cleanup to prevent unbounded memory growth."""
    __slots__ = ('cooldown', 'last_alert', '_last_clean_time')

    def __init__(self, cooldown_minutes=30):
        self.cooldown = cooldown_minutes * 60
        self.last_alert = {}
        self._last_clean_time = time.time()

    def _clean_expired(self):
        """v8.0: Remove entries older than 2x cooldown period."""
        now = time.time()
        if now - self._last_clean_time < 300:  # clean every 5 minutes
            return
        self._last_clean_time = now
        max_age = self.cooldown * 2
        old_count = len(self.last_alert)
        self.last_alert = {k: t for k, t in self.last_alert.items() if now - t < max_age}
        cleaned = old_count - len(self.last_alert)
        if cleaned:
            print(f"🧹 Cooldown cleanup: removed {cleaned} expired entries")

    def can_alert(self, stock_code):
        """Check if enough time has passed since last alert. Does NOT set timestamp."""
        self._clean_expired()  # v8.0: periodic cleanup
        now = time.time()
        return now - self.last_alert.get(stock_code, 0) >= self.cooldown

    def mark_alerted(self, stock_code):
        """Record that an alert was successfully sent for this stock."""
        self.last_alert[stock_code] = time.time()

    def reset(self, stock_code):
        self.last_alert.pop(stock_code, None)

async def safe_forward(client, channel, message, max_retries=3):
    """v8.0: Returns False on final failure instead of raising.
    v8.3: Added backoff delay between non-FloodWait retries."""
    for attempt in range(max_retries):
        try:
            await client.forward_messages(channel, message)
            return True
        except errors.FloodWaitError as e:
            await asyncio.sleep(e.seconds + 1)
        except Exception as e:
            if attempt >= max_retries - 1:
                print(f"⚠️ safe_forward failed after {max_retries} retries: {e}")
                return False
            await asyncio.sleep(1 * (attempt + 1))  # v8.3: backoff 1s, 2s
    return False


async def safe_send(client, channel, text, max_retries=3, **kwargs):
    """v8.0: Returns False on final failure instead of raising.
    v8.3: Added backoff delay between non-FloodWait retries."""
    for attempt in range(max_retries):
        try:
            await client.send_message(channel, text, **kwargs)
            return True
        except errors.FloodWaitError as e:
            await asyncio.sleep(e.seconds + 1)
        except Exception as e:
            if attempt >= max_retries - 1:
                print(f"⚠️ safe_send failed after {max_retries} retries: {e}")
                return False
            await asyncio.sleep(1 * (attempt + 1))  # v8.3: backoff 1s, 2s
    return False

# ============================================================
# BOT 3: STOCK ALERT PROCESSING (v8.3: extracted as background task)
# ============================================================
async def _process_stock_alerts(client, kis, cooldown, codes, text_codes, ocr_text, chat_name):
    """v8.3: Process stock alerts in background to avoid blocking the message handler.
    Previously this ran inline in the handler, causing message loss during API calls."""
    try:
        for code in codes:
            if not cooldown.can_alert(code):
                continue

            mkt = stock_universe.lookup_market(code)

            price_info = await kis.get_stock_price(code, market_code=mkt)
            if not price_info:
                continue

            if price_info.get("acml_tr_pbmn", 0) < VOLUME_THRESHOLD:
                continue

            api_name = (price_info.get("name") or "").strip()
            universe_name = stock_universe.lookup_name(code) or ""
            name = universe_name or api_name or code

            rolling_vol = await kis.get_rolling_volume(code, market_code=mkt, minutes=20)
            daily_closes = await kis.get_daily_prices(code, market_code=mkt, count=60)
            risk_label, risk_emoji = compute_risk_level(price_info)
            ma_state, ma_detail = compute_ma_state(daily_closes, price_info.get("price", 0))

            change_val = _safe_float(price_info.get("change_rate", "0"))
            direction = "🔺" if change_val > 0 else ("🔻" if change_val < 0 else "▫")

            from_ocr = ocr_text and code not in text_codes
            source_tag = " [OCR]" if from_ocr else ""
            mkt_label = "KOSDAQ" if mkt == "Q" else "KOSPI"

            w52h = price_info.get("w52_hgpr", 0)
            w52l = price_info.get("w52_lwpr", 0)
            current_price = price_info.get("price", 0)
            w52_pos_pct = (
                (current_price - w52l) / (w52h - w52l) * 100
                if w52h > w52l > 0 else 50
            )

            print(
                f"  💰{source_tag} [{code}/{mkt_label}] {name} {current_price:,}원 "
                f"({change_val:+.2f}%) 20분거래대금: {rolling_vol/100_000_000:.2f}억 "
                f"52주위치: {w52_pos_pct:.0f}%"
            )

            # v8.1: "Watch" header when all conditions met
            is_bullish_ma = ma_state.startswith("🟢")
            is_below_60pct = w52_pos_pct < 60
            is_volume_over_100b = price_info.get("acml_tr_pbmn", 0) > 10_000_000_000
            is_safe_risk = risk_emoji in ("🟡", "🟢")

            if is_bullish_ma and is_below_60pct and is_volume_over_100b and is_safe_risk:
                header = (
                    f"👀🔥 **WATCH — {name}** "
                    f"(MA강세 | 52주 {w52_pos_pct:.0f}% | "
                    f"거래대금 {price_info.get('acml_tr_pbmn', 0)/100_000_000:.0f}억 | "
                    f"RISK {risk_emoji})"
                )
            elif w52_pos_pct <= 30:
                header = f"🔻📉 **52주 저점 근접 — {name}** (52주 {w52_pos_pct:.0f}% 위치)"
            elif w52_pos_pct <= 50:
                header = f"⚠️📉 **52주 하단부 — {name}** (52주 {w52_pos_pct:.0f}% 위치)"
            else:
                header = f"🔔 **종목 알림 ({name})**"

            alert_lines = [
                header,
                "",
                f"📌 **{name}** ({code} | {mkt_label})",
                f"💰 현재가: {current_price:,}원 {direction} {change_val:+.2f}%",
                f"📈 시가: {price_info.get('open_price', 0):,} | "
                f"고가: {price_info.get('high_price', 0):,} | "
                f"저가: {price_info.get('low_price', 0):,}",
                f"📊 거래량: {price_info.get('acml_vol', 0):,}주",
                f"💵 누적거래대금: {price_info.get('acml_tr_pbmn', 0)/100_000_000:.1f}억",
                f"💵 20분 거래대금: {rolling_vol/100_000_000:.1f}억",
                "",
                f"⚠️ RISK: {risk_label}",
                f"📉 이동평균: {ma_state}",
            ]
            if ma_detail:
                alert_lines.append(f"  ➡ {ma_detail}")

            if w52h and w52l:
                alert_lines.append(f"📍 52주: {w52l:,} ~ {w52h:,} (현재 {w52_pos_pct:.0f}% 위치)")

            ocr_label = " 🔍(OCR)" if from_ocr else ""
            alert_lines.extend([
                "",
                f"💬 출처: {chat_name}{ocr_label}",
                f"⏰ {datetime.now().strftime('%H:%M:%S')}",
            ])

            try:
                ok = await safe_send(client, VOLUME_ALERT_CHANNEL, "\n".join(alert_lines), link_preview=False)
                if ok:
                    cooldown.mark_alerted(code)
                    print(f"  🚨 ALERT: {name} ({code}/{mkt_label}) ✅")
                else:
                    print(f"  ❌ Alert send failed: {name} ({code}/{mkt_label})")
            except Exception as e:
                print(f"  ❌ Alert failed: {e}")
            await asyncio.sleep(0.3)

    except Exception as e:
        print(f"  ❌ Stock alert background task error: {e}")
        import traceback
        traceback.print_exc()


# ============================================================
# MAIN
# ============================================================
async def main():
    start_time = time.time()
    print("=" * 50)
    print(" Telegram Monitor v8.3")
    print(" BOT1: Filter+Dedup (no crypto) -> @my_filtered_news")
    print(" BOT2: 실적/공시 -> @jason_earnings")
    print(" BOT3: 종목별 시세/거래대금/RISK/MA + OCR -> @alerts_forme")
    print("=" * 50)

    if not all([API_ID, API_HASH, SESSION_STRING, TARGET_CHANNEL]):
        missing = [v for v, val in [
            ("API_ID", API_ID), ("API_HASH", API_HASH),
            ("SESSION_STRING", SESSION_STRING), ("TARGET_CHANNEL", TARGET_CHANNEL),
        ] if not val]
        print(f"❌ Missing env vars: {', '.join(missing)}")
        return

    kis_ok = all([KIS_APP_KEY, KIS_APP_SECRET, VOLUME_ALERT_CHANNEL])
    if not kis_ok:
        print("⚠️ KIS API or VOLUME_ALERT_CHANNEL not set -- stock alerts disabled")
    if not EARNINGS_CHANNEL:
        print("⚠️ EARNINGS_CHANNEL not set -- earnings filter disabled")

    kis = KISApi() if kis_ok else None
    detector = DuplicateDetector(threshold=SIMILARITY_THRESHOLD)
    cooldown = AlertCooldown(cooldown_minutes=30)

    # v7.9: retry master file download
    print("📥 Loading stock universe from KIS master files...")
    await stock_universe.load_with_retry()
    if not stock_universe.all_codes:
        print("⚠️ Stock universe empty - falling back without name matching")

    client = TelegramClient(
        StringSession(SESSION_STRING), API_ID, API_HASH,
        connection_retries=5, retry_delay=2,
    )
    await client.start()
    me = await client.get_me()
    print(f"✅ Connected: {me.first_name} (@{me.username})")

    if kis:
        tok = await kis.get_token()
        if tok:
            print("✅ KIS API connected")
        else:
            print("⚠️ KIS API token failed -- stock alerts disabled")
            kis = None

    monitored_ids = set()
    exclude_ids = set()
    for ch in [TARGET_CHANNEL, EARNINGS_CHANNEL, VOLUME_ALERT_CHANNEL]:
        if not ch:
            continue
        try:
            ent = await client.get_entity(ch)
            exclude_ids.add(utils.get_peer_id(ent))
            print(f"  ✅ Output channel: {ch}")
        except Exception as e:
            print(f"  ❌ Cannot find channel {ch}: {e}")

    async def refresh_dialogs():
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
                    label = "📺 Channel" if is_channel else "👥 Group"
                    print(f"  {label}: {entity.title} (id: {peer_id})")
                    added += 1
        stale = monitored_ids - current_ids - exclude_ids
        if stale:
            monitored_ids.difference_update(stale)
            print(f"  🧹 Pruned {len(stale)} stale chats")
        return added

    await refresh_dialogs()
    print(f"📊 Monitoring {len(monitored_ids)} chats")
    print(f"📬 All unique (no crypto) -> {TARGET_CHANNEL}")
    if EARNINGS_CHANNEL:
        print(f"📈 실적/공시 -> {EARNINGS_CHANNEL}")
    if kis:
        print(f"🚨 종목 알림 (시세/거래대금/RISK/MA) -> {VOLUME_ALERT_CHANNEL}")
    if OCR_AVAILABLE:
        print("🔍 OCR enabled: image stock detection active")

    bg_tasks = []
    _alert_tasks = set()  # v8.3: prevent background tasks from being garbage-collected
    shutdown_event = asyncio.Event()

    def _signal_handler():
        """v8.0: Properly schedule async disconnect instead of sync call."""
        print("🛑 Received shutdown signal...")
        shutdown_event.set()
        # v8.0: Schedule the coroutine instead of calling synchronously
        asyncio.ensure_future(client.disconnect())

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
                    f"💚 Heartbeat: uptime={up//3600}h{(up%3600)//60}m | "
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
                print("🔄 Refreshing dialog list...")
                added = await refresh_dialogs()
                print(f"  ✅ {added} new, total: {len(monitored_ids)}")
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
            except Exception as e:
                print(f"  ⚠️ get_peer_id failed for {getattr(chat, 'title', '?')}: {e}")
                return
            if peer_id in exclude_ids:
                return
            if peer_id not in monitored_ids:
                if isinstance(chat, (Channel, Chat)):
                    monitored_ids.add(peer_id)
                    print(f"  🆕 Dynamically added: {getattr(chat, 'title', 'Unknown')}")
                else:
                    return

            chat_name = getattr(chat, "title", None) or "Unknown"  # v8.0: handle None/empty
            msg = (event.message.text or "").strip()  # v8.2: strip whitespace to prevent silent drops
            has_media = event.message.media is not None

            # ====== OCR (v7.9: runs regardless of KIS config) ======
            # v8.3: OCR with timeout to prevent handler blocking on large images
            ocr_text = ""
            if has_media and OCR_AVAILABLE:
                try:
                    ocr_text = await asyncio.wait_for(
                        extract_text_from_image(client, event.message, chat_name=chat_name),
                        timeout=10.0  # v8.3: 10s max for OCR
                    )
                except asyncio.TimeoutError:
                    print(f"  ⚠️ [{chat_name}] OCR timed out (>10s), skipping image text")
                    ocr_text = ""
                if has_media and not ocr_text and isinstance(event.message.media, (MessageMediaPhoto,)):
                    print(f"  🔍 [{chat_name}] OCR returned empty for photo (may have failed silently)")

            combined_text = f"{msg}\n{ocr_text}".strip() if ocr_text else msg

            # ====== BOT 1: Unique messages (no crypto) ======
            if not msg and not ocr_text:
                if has_media:
                    # v8.0: safe_forward/safe_send now return False instead of raising
                    ok = await safe_forward(client, TARGET_CHANNEL, event.message)
                    if ok:
                        print(f"  📎 [{chat_name}] Media-only forwarded")
                    else:
                        icon = "📺" if isinstance(chat, Channel) and chat.broadcast else "👥"
                        await safe_send(client, TARGET_CHANNEL, f"📎 {icon}**{chat_name}** [미디어 메시지]", link_preview=False)
                        print(f"  📎 [{chat_name}] Media-only sent as text fallback")
                return

            if contains_crypto_keyword(combined_text):
                # v8.3: Log which keyword triggered, to debug false positives
                lower = combined_text.lower()
                triggered = [kw for kw in _CRYPTO_LONG if kw in lower]
                if not triggered and _CRYPTO_SHORT_PATTERN:
                    m = _CRYPTO_SHORT_PATTERN.search(lower)
                    if m:
                        triggered = [m.group()]
                print(f"  🚫 [{chat_name}] Crypto filtered (keyword: {triggered[:3]}): {combined_text[:80]}...")
                return

            if await detector.is_duplicate(combined_text):
                print(f"  ♻️ [{chat_name}] Duplicate ({len(combined_text)} chars): {combined_text[:80]}...")
                return

            print(f"📨 [{chat_name}] Unique msg ({len(combined_text)} chars)")
            # v8.0: safe_forward/safe_send return bool instead of raising
            forwarded = await safe_forward(client, TARGET_CHANNEL, event.message)
            if forwarded:
                print(f"  ✅ Forwarded to {TARGET_CHANNEL}")
            else:
                icon = "📺" if isinstance(chat, Channel) and chat.broadcast else "👥"
                forwarded = await safe_send(client, TARGET_CHANNEL, f"{icon}**{chat_name}**\n{combined_text[:500]}", link_preview=False)
            if not forwarded:
                print(f"  ❌ FAILED to deliver to {TARGET_CHANNEL}!")

            # ====== BOT 2: 실적/공시 ======
            if EARNINGS_CHANNEL and combined_text and contains_earnings_keyword(combined_text):
                matched = [kw for kw in _ALL_EARNINGS_LOWER if kw in combined_text.lower()][:5]
                print(f"  📈 실적/공시: {matched}")
                # v8.0: safe_forward/safe_send return bool
                ok = await safe_forward(client, EARNINGS_CHANNEL, event.message)
                if ok:
                    print(f"  ✅ -> {EARNINGS_CHANNEL}")
                else:
                    await safe_send(
                        client, EARNINGS_CHANNEL,
                        f"📈 **[실적/공시]** {chat_name}\n🔑 {', '.join(matched)}\n{combined_text[:500]}",
                        link_preview=False,
                    )

            # ====== BOT 3: 종목 알림 (v8.3: runs as background task to not block BOT 1) ======
            if kis and combined_text:
                codes = set(stock_universe.find_stocks_in_text(combined_text))
                text_codes = set(stock_universe.find_stocks_in_text(msg)) if msg else set()
                if codes:
                    code_names = [f"{c}({stock_universe.lookup_name(c) or '?'})" for c in codes]
                    print(f"  🔎 [{chat_name}] 종목 감지: {', '.join(code_names)}")
                    # v8.3: Process stock alerts in background so handler returns quickly
                    task = asyncio.create_task(
                        _process_stock_alerts(
                            client, kis, cooldown, codes, text_codes,
                            ocr_text, chat_name,
                        )
                    )
                    _alert_tasks.add(task)
                    task.add_done_callback(_alert_tasks.discard)

        except errors.FloodWaitError as e:
            # v8.2: Log the lost message context before sleeping
            lost_ctx = getattr(event.message, 'text', '')
            lost_preview = (lost_ctx or '')[:60]
            print(f"  ⏳ FloodWait {e.seconds}s — message lost: {lost_preview}...")
            await asyncio.sleep(e.seconds + 1)
        except Exception as e:
            print(f"  ❌ Handler error: {e}")
            import traceback
            traceback.print_exc()

    print(f"🎧 Listening... (v8.3)")
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
        print("✅ Cleanup complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bye!")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
