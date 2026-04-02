"""
Telegram Channel & Group Monitor v8.6 (simplified)
====================================================
BOT 1: All unique messages (no crypto) -> TARGET_CHANNEL
BOT 2: 실적/공시 keyword messages -> EARNINGS_CHANNEL
BOT 3: 종목 언급 시 -> 시세/거래대금/RISK/MA/외국인/프로그램 알림 -> VOLUME_ALERT_CHANNEL
BOT 4: 외국인/프로그램 대량매매 트리거 알림 -> INVESTOR_ALERT_CHANNEL

v8.6: Code simplification (no behaviour changes)
  - Generic _api_get() eliminates ~80 lines of duplicated KIS error handling
  - Shared _dispatch_bot2_bot3() deduplicates handler + edit_handler logic
  - _forward_to_earnings() extracted as reusable standalone function
  - _with_retry() helper merges safe_forward/safe_send retry pattern
  - Fixed investor scan double program_data fetch
  - Changelog moved to CHANGELOG.md
"""

import os, re, sys, io, signal, zipfile, asyncio, hashlib, time, aiohttp, traceback
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
INVESTOR_ALERT_CHANNEL = os.environ.get("INVESTOR_ALERT_CHANNEL")
SIMILARITY_THRESHOLD = float(os.environ.get("SIMILARITY_THRESHOLD", "0.85"))

KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")
VOLUME_THRESHOLD = int(os.environ.get("VOLUME_THRESHOLD", "1000000000"))
FRGN_NET_THRESHOLD = int(os.environ.get("FRGN_NET_THRESHOLD", "5000000000"))
PRGM_NET_THRESHOLD = int(os.environ.get("PRGM_NET_THRESHOLD", "3000000000"))
INVESTOR_SCAN_INTERVAL = int(os.environ.get("INVESTOR_SCAN_INTERVAL", "900"))
KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"
API_TIMEOUT = int(os.environ.get("API_TIMEOUT", "30"))

KIS_KOSPI_MST_URL = "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip"
KIS_KOSDAQ_MST_URL = "https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip"
_KOSPI_TAIL_LEN = 228
_KOSDAQ_TAIL_LEN = 222

# ============================================================
# KEYWORD SETS
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

EARNINGS_ANCHOR_KEYWORDS = frozenset([
    "공시", "공시내용", "수시공시", "주요공시",
    "실적발표", "실적공시", "잠정실적", "잠정치", "확정치",
    "사업보고서", "분기보고서", "반기보고서",
    "연결기준", "별도기준",
    "실적", "실적시즌", "실적쇼크", "실적서프라이즈",
    "어닝쇼크", "어닝서프라이즈",
])

EARNINGS_DATA_KEYWORDS = frozenset([
    "영업이익", "당기순이익", "순이익", "매출액", "매출",
    "영업손실", "순손실", "당기순손실",
    "적자전환", "흑자전환", "적자지속", "흑자지속",
    "매출총이익", "EBITDA", "EPS", "BPS",
    "영업이익률", "순이익률", "컨센서스",
    "전년대비", "전분기대비", "YoY", "QoQ",
    "1분기", "2분기", "3분기", "4분기", "1Q", "2Q", "3Q", "4Q",
    "반기실적", "연간실적",
    "ROE", "ROA", "PER", "PBR",
    "판매량", "판매실적", "수주", "수주잔고", "수주액",
    "이익", "순익",
])

HIGH_CONFIDENCE_EARNINGS_KEYWORDS = frozenset([
    "영업이익", "당기순이익", "순이익", "순익", "매출액",
    "영업손실", "순손실", "당기순손실", "매출",
])

# Pre-compiled lookups
_CRYPTO_LOWER = frozenset(k.lower() for k in CRYPTO_KEYWORDS)
_CRYPTO_SHORT = frozenset(k for k in _CRYPTO_LOWER if len(k) <= 4 and k.isascii())
_CRYPTO_LONG = _CRYPTO_LOWER - _CRYPTO_SHORT
_CRYPTO_SHORT_PATTERN = re.compile(
    r'\b(' + '|'.join(re.escape(k) for k in sorted(_CRYPTO_SHORT, key=len, reverse=True)) + r')\b',
    re.IGNORECASE | re.ASCII
) if _CRYPTO_SHORT else None
_ANCHOR_LOWER = frozenset(k.lower() for k in EARNINGS_ANCHOR_KEYWORDS)
_DATA_LOWER = frozenset(k.lower() for k in EARNINGS_DATA_KEYWORDS)
_HIGH_CONF_LOWER = frozenset(k.lower() for k in HIGH_CONFIDENCE_EARNINGS_KEYWORDS)
_ALL_EARNINGS_LOWER = _ANCHOR_LOWER | _DATA_LOWER | _HIGH_CONF_LOWER

FINANCIAL_NUMBER_PATTERN = re.compile(r'[\d,]+\s*(억|조|원|백만|천만)')
STOCK_CODE_PATTERN = re.compile(r'\b(\d{6})\b')


# ============================================================
# DYNAMIC STOCK UNIVERSE
# ============================================================
class StockUniverse:
    __slots__ = (
        'name_to_code', 'code_to_name', 'code_to_market',
        'all_codes', 'last_refresh', 'refresh_interval', '_name_buckets',
    )

    def __init__(self):
        self.name_to_code = {}
        self.code_to_name = {}
        self.code_to_market = {}
        self.all_codes = set()
        self.last_refresh = 0
        self.refresh_interval = 86400
        self._name_buckets = {}

    def _parse_master_file(self, raw_bytes, tail_len, market_code):
        n2c, c2n, c2m = {}, {}, {}
        text = raw_bytes.decode("cp949", errors="ignore")
        for row in text.strip().split("\n"):
            if not row or len(row) <= tail_len + 21:
                continue
            front = row[:len(row) - tail_len]
            code_match = re.search(r'(\d{6})', front[0:9].strip())
            if not code_match:
                continue
            code = code_match.group(1)
            name = front[21:].strip()
            if name:
                n2c[name] = code
                c2n[code] = name
                c2m[code] = market_code
        return n2c, c2n, c2m

    def _build_index(self):
        self._name_buckets = {}
        for name in self.name_to_code:
            if len(name) < 2:
                continue
            self._name_buckets.setdefault(len(name), []).append(name)

    async def load(self, session=None):
        close_session = session is None
        if close_session:
            session = aiohttp.ClientSession()
        try:
            all_n2c, all_c2n, all_c2m = {}, {}, {}
            for url, market, mkt_code, tail_len in [
                (KIS_KOSPI_MST_URL, "KOSPI", "J", _KOSPI_TAIL_LEN),
                (KIS_KOSDAQ_MST_URL, "KOSDAQ", "Q", _KOSDAQ_TAIL_LEN),
            ]:
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=API_TIMEOUT)) as resp:
                        if resp.status != 200:
                            print(f"⚠️ Failed to download {market} master: HTTP {resp.status}")
                            continue
                        data = await resp.read()
                    count = 0
                    with zipfile.ZipFile(io.BytesIO(data)) as zf:
                        for filename in zf.namelist():
                            n2c, c2n, c2m = self._parse_master_file(zf.read(filename), tail_len, mkt_code)
                            count += len(c2n)
                            for name, code in n2c.items():
                                if name in all_n2c and all_n2c[name] != code:
                                    old_code = all_n2c.pop(name)
                                    old_mkt = all_c2m.get(old_code, "J")
                                    all_n2c[f"{name}({old_mkt})"] = old_code
                                    all_n2c[f"{name}({mkt_code})"] = code
                                else:
                                    all_n2c[name] = code
                            all_c2n.update(c2n)
                            all_c2m.update(c2m)
                    print(f"✅ {market} master loaded: {count} stocks")
                except Exception as e:
                    print(f"❌ Error loading {market} master: {e}")
            if all_n2c:
                self.name_to_code, self.code_to_name, self.code_to_market = all_n2c, all_c2n, all_c2m
                self.all_codes = set(all_c2n.keys())
                self._build_index()
                self.last_refresh = time.time()
                print(f"📊 Stock universe: {len(self.name_to_code)} names, {len(self.all_codes)} codes")
            else:
                print("⚠️ Stock universe empty - master files may have changed format")
        finally:
            if close_session:
                await session.close()

    async def load_with_retry(self, session=None, max_retries=3, retry_delay=60):
        for attempt in range(max_retries):
            await self.load(session)
            if self.all_codes:
                return
            if attempt < max_retries - 1:
                delay = retry_delay * (2 ** attempt)
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
        if not text:
            return []
        found = set()
        for m in STOCK_CODE_PATTERN.finditer(text):
            code = m.group()
            if code != "000000" and code in self.all_codes:
                found.add(code)
        text_len = len(text)
        text_upper = text.upper()
        for name_len, names in self._name_buckets.items():
            if name_len > text_len:
                continue
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
def _run_tesseract(image_bytes):
    img = Image.open(io.BytesIO(image_bytes))
    return pytesseract.image_to_string(img, lang="kor+eng").strip()


async def extract_text_from_image(client, message, chat_name=""):
    if not OCR_AVAILABLE or not message.media:
        return ""
    media = message.media
    is_photo = isinstance(media, MessageMediaPhoto)
    is_image_doc = (isinstance(media, MessageMediaDocument) and media.document
                    and (getattr(media.document, 'mime_type', '') or '').startswith('image/'))
    if not is_photo and not is_image_doc:
        return ""
    try:
        image_bytes = await client.download_media(message, bytes)
        if not image_bytes:
            return ""
        loop = asyncio.get_running_loop()
        text = await loop.run_in_executor(None, _run_tesseract, image_bytes)
        if text:
            print(f"🔍 OCR extracted {len(text)} chars from image")
        return text
    except Exception as e:
        ctx = f" [{chat_name}]" if chat_name else ""
        print(f"⚠️ OCR error{ctx} (msg_id={message.id}): {e}")
        return ""


# ============================================================
# KIS API (SIMPLIFIED: generic _api_get eliminates repeated error handling)
# ============================================================
def _safe_int(val, default=0):
    if val is None:
        return default
    try:
        return int(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return default


def _safe_float(val, default=0.0):
    if val is None:
        return default
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return default


class KISApi:
    __slots__ = ('access_token', 'token_expires', 'session', '_semaphore')

    def __init__(self):
        self.access_token = None
        self.token_expires = 0
        self.session = None
        self._semaphore = asyncio.Semaphore(10)

    async def _new_session(self):
        if self.session and not self.session.closed:
            try: await self.session.close()
            except Exception: pass
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=API_TIMEOUT))

    async def ensure_session(self):
        if self.session is None or self.session.closed:
            await self._new_session()

    async def _safe_request(self, method, url, **kwargs):
        await self.ensure_session()
        try:
            fn = self.session.get if method == "GET" else self.session.post
            return await fn(url, **kwargs)
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
            print(f"⚠️ Session error ({e}), recreating...")
            await self._new_session()
            fn = self.session.get if method == "GET" else self.session.post
            return await fn(url, **kwargs)

    async def get_token(self):
        now = time.time()
        if self.access_token and now < self.token_expires:
            return self.access_token
        body = {"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET}
        try:
            async with await self._safe_request("POST", f"{KIS_BASE_URL}/oauth2/tokenP", json=body) as resp:
                if resp.status != 200:
                    print(f"❌ KIS 토큰 HTTP {resp.status}")
                    return None
                try: data = await resp.json()
                except (aiohttp.ContentTypeError, ValueError) as e:
                    print(f"❌ KIS 토큰 JSON 파싱 실패: {e}")
                    return None
            if "access_token" in data:
                self.access_token = data["access_token"]
                expires_in = int(data.get("expires_in", 85000))
                self.token_expires = now + expires_in - 300
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
            "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET,
            "tr_id": tr_id, "Content-Type": "application/json; charset=utf-8",
        }

    async def _api_get(self, path, tr_id, params, label, stock_code, output_key="output"):
        """Generic KIS API GET — handles token, rate-limit, HTTP, JSON, rt_cd checks.
        Returns the output dict/list or None on any failure."""
        token = await self.get_token()
        if not token:
            return None
        async with self._semaphore:
            try:
                async with await self._safe_request(
                    "GET", f"{KIS_BASE_URL}{path}",
                    headers=self._headers(tr_id), params=params
                ) as resp:
                    if resp.status != 200:
                        print(f"⚠️ {label} HTTP {resp.status} [{stock_code}]")
                        return None
                    try: data = await resp.json()
                    except (aiohttp.ContentTypeError, ValueError) as e:
                        print(f"⚠️ {label} JSON 파싱 실패 [{stock_code}]: {e}")
                        return None
                if data.get("rt_cd") == "0":
                    return data.get(output_key)
                print(f"⚠️ {label} 실패 [{stock_code}]: {data.get('msg1', '')}")
                return None
            except asyncio.TimeoutError:
                print(f"⚠️ {label} 타임아웃 [{stock_code}]")
                return None
            except Exception as e:
                print(f"❌ {label} 에러 [{stock_code}]: {e}")
                return None

    async def get_stock_price(self, stock_code, market_code="J"):
        params = {"FID_COND_MRKT_DIV_CODE": market_code, "FID_INPUT_ISCD": stock_code}
        out = await self._api_get(
            "/uapi/domestic-stock/v1/quotations/inquire-price",
            "FHKST01010100", params, "시세", stock_code
        )
        if not out:
            return None
        price = _safe_int(out.get("stck_prpr"))
        if price <= 0:
            print(f"⚠️ 시세 price=0 [{stock_code}], skipping")
            return None
        return {
            "name": out.get("hts_kor_isnm", stock_code),
            "price": price,
            "change_rate": out.get("prdy_ctrt", "0"),
            "change_sign": out.get("prdy_vrss_sign", "3"),
            "high_price": _safe_int(out.get("stck_hgpr")),
            "low_price": _safe_int(out.get("stck_lwpr")),
            "open_price": _safe_int(out.get("stck_oprc")),
            "prev_close": _safe_int(out.get("stck_sdpr")),
            "acml_tr_pbmn": _safe_int(out.get("acml_tr_pbmn")),
            "acml_vol": _safe_int(out.get("acml_vol")),
            "per": out.get("per", "N/A"), "pbr": out.get("pbr", "N/A"),
            "w52_hgpr": _safe_int(out.get("w52_hgpr")),
            "w52_lwpr": _safe_int(out.get("w52_lwpr")),
        }

    async def get_rolling_volume(self, stock_code, market_code="J", minutes=20):
        now = datetime.now()
        params = {
            "FID_ETC_CLS_CODE": "", "FID_COND_MRKT_DIV_CODE": market_code,
            "FID_INPUT_ISCD": stock_code, "FID_INPUT_HOUR_1": now.strftime("%H%M%S"),
            "FID_PW_DATA_INCU_YN": "N",
        }
        items = await self._api_get(
            "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
            "FHKST03010200", params, "분봉", stock_code, output_key="output2"
        )
        if not items:
            return 0
        cutoff = now - timedelta(minutes=minutes)
        total = 0
        for item in items:
            ts = item.get("stck_cntg_hour", "")
            if ts and len(ts) >= 4:
                try:
                    candle_dt = now.replace(hour=int(ts[:2]), minute=int(ts[2:4]), second=0, microsecond=0)
                    if candle_dt < cutoff:
                        break
                except (ValueError, IndexError):
                    pass
            total += _safe_int(item.get("cntg_vol")) * _safe_int(item.get("stck_prpr"))
        return total

    async def get_daily_prices(self, stock_code, market_code="J", count=60):
        today = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")
        params = {
            "FID_COND_MRKT_DIV_CODE": market_code, "FID_INPUT_ISCD": stock_code,
            "FID_INPUT_DATE_1": start_date, "FID_INPUT_DATE_2": today,
            "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "0",
        }
        items = await self._api_get(
            "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            "FHKST03010100", params, "일봉", stock_code, output_key="output2"
        )
        if not items:
            return []
        dated = []
        for item in items[:count]:
            close = _safe_int(item.get("stck_clpr"))
            date_str = item.get("stck_bsop_date", "")
            if close > 0 and date_str:
                dated.append((date_str, close))
        dated.sort(key=lambda x: x[0], reverse=True)
        return [close for _, close in dated]

    async def get_investor_trend(self, stock_code, market_code="J"):
        output2 = await self._api_get(
            "/uapi/domestic-stock/v1/quotations/investor-trend-estimate",
            "HHPTJ04160200", {"MKSC_SHRN_ISCD": stock_code},
            "투자자추정", stock_code, output_key="output2"
        )
        if not output2:
            return None
        row = output2[0] if isinstance(output2, list) else output2
        return {
            "frgn_ntby_qty": _safe_int(row.get("frgn_ntby_qty")),
            "frgn_ntby_tr_pbmn": _safe_int(row.get("frgn_ntby_tr_pbmn")),
            "orgn_ntby_qty": _safe_int(row.get("orgn_ntby_qty")),
            "orgn_ntby_tr_pbmn": _safe_int(row.get("orgn_ntby_tr_pbmn")),
        }

    async def get_program_trade(self, stock_code, market_code="J"):
        output = await self._api_get(
            "/uapi/domestic-stock/v1/quotations/program-trade-by-stock",
            "FHPPG04650101",
            {"FID_COND_MRKT_DIV_CODE": market_code, "FID_INPUT_ISCD": stock_code},
            "프로그램매매", stock_code
        )
        if not output:
            return None
        row = output[0] if isinstance(output, list) else output
        return {
            "prgm_ntby_qty": _safe_int(row.get("ntby_qty")),
            "prgm_ntby_tr_pbmn": _safe_int(row.get("ntby_tr_pbmn")),
            "prgm_seln_qty": _safe_int(row.get("seln_qty")),
            "prgm_shnu_qty": _safe_int(row.get("shnu_qty")),
        }

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()


# ============================================================
# RISK LEVEL & MOVING AVERAGE
# ============================================================
def compute_risk_level(price_info):
    try:
        change_rate = abs(_safe_float(price_info.get("change_rate", "0")))
        price = price_info.get("price", 0)
        if not isinstance(price, (int, float)) or price <= 0:
            return "❓ N/A", "❓"
        w52_high, w52_low = price_info.get("w52_hgpr", 0), price_info.get("w52_lwpr", 0)
        position = max(0, min(100, (price - w52_low) / (w52_high - w52_low) * 100)) if w52_high > w52_low > 0 else 50
        score = 0
        if change_rate >= 10: score += 3
        elif change_rate >= 5: score += 2
        elif change_rate >= 3: score += 1
        if position >= 90: score += 2
        elif position >= 75: score += 1
        if position <= 10: score += 2
        elif position <= 25: score += 1
        if score >= 4: return "🔴 HIGH", "🔴"
        elif score >= 2: return "🟡 MEDIUM", "🟡"
        else: return "🟢 LOW", "🟢"
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
        parts, signals = [], {"bullish": 0, "bearish": 0}
        for label, ma in [("5일선", ma5), ("20일선", ma20), ("60일선", ma60)]:
            if ma is None:
                continue
            above = price > ma
            parts.append(f"{label}({ma:,.0f}) {'위' if above else '아래'}")
            signals["bullish" if above else "bearish"] += 1
        cross = ""
        if ma20 and len(closes) >= 21:
            prev_ma5, prev_ma20 = sum(closes[1:6]) / 5, sum(closes[1:21]) / 20
            if prev_ma5 <= prev_ma20 and ma5 > ma20:
                cross = "🌟 골든크로스(5/20)"
                signals["bullish"] += 2
            elif prev_ma5 >= prev_ma20 and ma5 < ma20:
                cross = "💀 데드크로스(5/20)"
                signals["bearish"] += 2
        b, r = signals["bullish"], signals["bearish"]
        if b >= 4: state = "🟢 강세 (Strong Bullish)"
        elif b > r: state = "🟢 상승추세 (Bullish)"
        elif r >= 4: state = "🔴 약세 (Strong Bearish)"
        elif r > b: state = "🔴 하락추세 (Bearish)"
        else: state = "🟡 횡보 (Neutral)"
        ma_detail = " | ".join(parts)
        if cross:
            ma_detail += f" | {cross}"
        return state, ma_detail
    except Exception:
        return "❓ 계산 불가", ""


# ============================================================
# KEYWORD MATCHING
# ============================================================
def contains_crypto_keyword(text):
    if not text:
        return False
    lower = text.lower()
    if any(kw in lower for kw in _CRYPTO_LONG):
        return True
    if _CRYPTO_SHORT_PATTERN and _CRYPTO_SHORT_PATTERN.search(lower):
        return True
    return False


def contains_earnings_keyword(text):
    if not text:
        return False
    lower = text.lower()
    if any(kw in lower for kw in _ANCHOR_LOWER) and any(kw in lower for kw in _DATA_LOWER):
        return True
    if any(kw in lower for kw in _HIGH_CONF_LOWER) and FINANCIAL_NUMBER_PATTERN.search(text):
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
    return sum((1 << i) for i in range(hashbits) if v[i] > 0)


def _hamming_distance(a, b):
    x = a ^ b
    return x.bit_count() if hasattr(x, 'bit_count') else bin(x).count('1')


class DuplicateDetector:
    __slots__ = ('max_history', 'ttl_seconds', 'hamming_threshold',
                 'seen_hashes', 'seen_simhashes', 'stats', '_lock', '_last_clean_time')

    def __init__(self, threshold=0.85, max_history=500, ttl_hours=6):
        self.max_history = max_history
        self.ttl_seconds = ttl_hours * 3600
        self.hamming_threshold = int((1 - threshold) * 64)
        self.seen_hashes = {}
        self.seen_simhashes = []
        self.stats = {"total": 0, "unique": 0, "duplicate": 0, "skipped": 0}
        self._lock = asyncio.Lock()
        self._last_clean_time = time.time()

    def _clean_old(self):
        now = time.time()
        if now - self._last_clean_time < 60:
            return
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
                        print(f"    🔎 SimHash match (hamming={dist}/{self.hamming_threshold}): {text[:60]}...")
                        return True
                self.seen_simhashes.append((text_simhash, now))
            self.seen_hashes[text_hash] = now
            self.stats["unique"] += 1
            return False

    def get_stats(self):
        return self.stats


# ============================================================
# ALERT COOLDOWN
# ============================================================
class AlertCooldown:
    __slots__ = ('cooldown', 'last_alert', '_last_clean_time', '_lock')

    def __init__(self, cooldown_minutes=30):
        self.cooldown = cooldown_minutes * 60
        self.last_alert = {}
        self._last_clean_time = time.time()
        self._lock = asyncio.Lock()

    def _clean_expired(self):
        now = time.time()
        if now - self._last_clean_time < 300:
            return
        self._last_clean_time = now
        max_age = self.cooldown * 2
        old_count = len(self.last_alert)
        self.last_alert = {k: t for k, t in self.last_alert.items() if now - t < max_age}
        cleaned = old_count - len(self.last_alert)
        if cleaned:
            print(f"🧹 Cooldown cleanup: removed {cleaned} expired entries")

    def can_alert(self, stock_code):
        self._clean_expired()
        return time.time() - self.last_alert.get(stock_code, 0) >= self.cooldown

    async def try_claim(self, stock_code):
        async with self._lock:
            if not self.can_alert(stock_code):
                return False
            self.last_alert[stock_code] = time.time()
            return True

    def mark_alerted(self, stock_code):
        self.last_alert[stock_code] = time.time()

    def reset(self, stock_code):
        self.last_alert.pop(stock_code, None)


# ============================================================
# RETRY HELPER (replaces separate safe_forward + safe_send)
# ============================================================
async def _with_retry(coro_fn, label="op", max_retries=3):
    """Generic retry wrapper with FloodWait + backoff handling."""
    for attempt in range(max_retries):
        try:
            return await coro_fn()
        except errors.FloodWaitError as e:
            await asyncio.sleep(e.seconds + 1)
        except Exception as e:
            if attempt >= max_retries - 1:
                print(f"⚠️ {label} failed after {max_retries} retries: {e}")
                return False
            await asyncio.sleep(1 * (attempt + 1))
    return False


async def safe_forward(client, channel, message, max_retries=3):
    return await _with_retry(
        lambda: client.forward_messages(channel, message),
        "safe_forward", max_retries
    )


async def safe_send(client, channel, text, max_retries=3, **kwargs):
    return await _with_retry(
        lambda: client.send_message(channel, text, **kwargs),
        "safe_send", max_retries
    )


# ============================================================
# SHARED: Forward to earnings channel (used by handler + edit_handler)
# ============================================================
async def _forward_to_earnings(client, msg_obj, chat_name, matched_kw, text, is_edit=False):
    """Forward a message to EARNINGS_CHANNEL. Used by both new msg and edit handlers."""
    tag = "실적/공시 수정" if is_edit else "실적/공시"
    emoji = "📈✏️" if is_edit else "📈"
    try:
        ok = await safe_forward(client, EARNINGS_CHANNEL, msg_obj)
        if ok:
            print(f"  ✅ {'Edited ' if is_edit else ''}-> {EARNINGS_CHANNEL}")
        else:
            await safe_send(
                client, EARNINGS_CHANNEL,
                f"{emoji} **[{tag}]** {chat_name}\n🔑 {', '.join(matched_kw)}\n{text[:3800]}",
                link_preview=False,
            )
    except Exception as e:
        print(f"  ❌ Earnings forward failed: {e}")


# ============================================================
# BOT 3: STOCK ALERT PROCESSING (background task)
# ============================================================
def _format_stock_alert(name, code, mkt, price_info, rolling_vol, ma_state, ma_detail,
                        risk_label, risk_emoji, investor_data, program_data, from_ocr, chat_name):
    """Build the alert message text. Pure function — no I/O."""
    current_price = price_info.get("price", 0)
    change_val = _safe_float(price_info.get("change_rate", "0"))
    direction = "🔺" if change_val > 0 else ("🔻" if change_val < 0 else "▫")
    mkt_label = "KOSDAQ" if mkt == "Q" else "KOSPI"
    w52h, w52l = price_info.get("w52_hgpr", 0), price_info.get("w52_lwpr", 0)
    w52_pos = max(0, min(100, (current_price - w52l) / (w52h - w52l) * 100)) if w52h > w52l > 0 else 50

    # Header logic
    is_bullish = ma_state.startswith("🟢")
    if is_bullish and w52_pos < 60 and price_info.get("acml_tr_pbmn", 0) > 10_000_000_000 and risk_emoji in ("🟡", "🟢"):
        header = (f"👀🔥 **WATCH — {name}** (MA강세 | 52주 {w52_pos:.0f}% | "
                  f"거래대금 {price_info.get('acml_tr_pbmn', 0)/100_000_000:.0f}억 | RISK {risk_emoji})")
    elif w52_pos <= 30:
        header = f"🔻📉 **52주 저점 근접 — {name}** (52주 {w52_pos:.0f}% 위치)"
    elif w52_pos <= 50:
        header = f"⚠️📉 **52주 하단부 — {name}** (52주 {w52_pos:.0f}% 위치)"
    else:
        header = f"🔔 **종목 알림 ({name})**"

    lines = [
        header, "",
        f"📌 **{name}** ({code} | {mkt_label})",
        f"💰 현재가: {current_price:,}원 {direction} {change_val:+.2f}%",
        f"📈 시가: {price_info.get('open_price', 0):,} | 고가: {price_info.get('high_price', 0):,} | 저가: {price_info.get('low_price', 0):,}",
        f"📊 거래량: {price_info.get('acml_vol', 0):,}주",
        f"💵 누적거래대금: {price_info.get('acml_tr_pbmn', 0)/100_000_000:.1f}억",
        f"💵 20분 거래대금: {rolling_vol/100_000_000:.1f}억", "",
        f"⚠️ RISK: {risk_label}",
        f"📉 이동평균: {ma_state}",
    ]
    if ma_detail:
        lines.append(f"  ➡ {ma_detail}")

    if investor_data or program_data:
        lines.append("")
    if investor_data:
        for label_icon, key, label_name in [
            ("🌍", "frgn_ntby_tr_pbmn", "외국인"), ("🏦", "orgn_ntby_tr_pbmn", "기관")
        ]:
            amt = investor_data.get(key, 0)
            d = "🔵순매수" if amt > 0 else ("🔴순매도" if amt < 0 else "—")
            lines.append(f"{label_icon} {label_name}: {d} {abs(amt)/100_000_000:.1f}억")
    if program_data:
        prgm_amt = program_data.get("prgm_ntby_tr_pbmn", 0)
        d = "🔵순매수" if prgm_amt > 0 else ("🔴순매도" if prgm_amt < 0 else "—")
        lines.append(f"🖥️ 프로그램: {d} {abs(prgm_amt)/100_000_000:.1f}억")

    if w52h and w52l:
        lines.append(f"📍 52주: {w52l:,} ~ {w52h:,} (현재 {w52_pos:.0f}% 위치)")

    ocr_label = " 🔍(OCR)" if from_ocr else ""
    lines.extend(["", f"💬 출처: {chat_name}{ocr_label}", f"⏰ {datetime.now().strftime('%H:%M:%S')}"])
    return "\n".join(lines), w52_pos


async def _process_stock_alerts(client, kis, cooldown, codes, text_codes, ocr_text, chat_name):
    """Process stock alerts in background to avoid blocking the message handler."""
    try:
        for code in codes:
            if not await cooldown.try_claim(code):
                continue
            mkt = stock_universe.lookup_market(code)
            price_info = await kis.get_stock_price(code, market_code=mkt)
            if not price_info:
                cooldown.reset(code)
                continue
            if price_info.get("acml_tr_pbmn", 0) < VOLUME_THRESHOLD:
                cooldown.reset(code)
                continue

            name = stock_universe.lookup_name(code) or (price_info.get("name") or "").strip() or code
            rolling_vol = await kis.get_rolling_volume(code, market_code=mkt, minutes=20)
            daily_closes = await kis.get_daily_prices(code, market_code=mkt, count=60)
            risk_label, risk_emoji = compute_risk_level(price_info)
            ma_state, ma_detail = compute_ma_state(daily_closes, price_info.get("price", 0))
            investor_data = await kis.get_investor_trend(code, market_code=mkt)
            program_data = await kis.get_program_trade(code, market_code=mkt)
            from_ocr = ocr_text and code not in text_codes

            alert_text, w52_pos = _format_stock_alert(
                name, code, mkt, price_info, rolling_vol, ma_state, ma_detail,
                risk_label, risk_emoji, investor_data, program_data, from_ocr, chat_name
            )
            mkt_label = "KOSDAQ" if mkt == "Q" else "KOSPI"
            src = " [OCR]" if from_ocr else ""
            print(f"  💰{src} [{code}/{mkt_label}] {name} {price_info['price']:,}원 "
                  f"({_safe_float(price_info.get('change_rate','0')):+.2f}%) "
                  f"20분거래대금: {rolling_vol/100_000_000:.2f}억 52주위치: {w52_pos:.0f}%")

            ok = await safe_send(client, VOLUME_ALERT_CHANNEL, alert_text, link_preview=False)
            if ok:
                cooldown.mark_alerted(code)
                print(f"  🚨 ALERT: {name} ({code}/{mkt_label}) ✅")
            else:
                print(f"  ❌ Alert send failed: {name} ({code}/{mkt_label})")
            await asyncio.sleep(0.3)
    except Exception as e:
        print(f"  ❌ Stock alert background task error: {e}")
        traceback.print_exc()


# ============================================================
# BOT 4: FOREIGNER/PROGRAM LARGE TRADE SCANNER
# ============================================================
class InvestorScanCooldown:
    __slots__ = ('cooldown', 'last_alert', '_lock')

    def __init__(self, cooldown_minutes=60):
        self.cooldown = cooldown_minutes * 60
        self.last_alert = {}
        self._lock = asyncio.Lock()

    async def try_claim(self, key):
        async with self._lock:
            now = time.time()
            if key in self.last_alert and (now - self.last_alert[key]) < self.cooldown:
                return False
            self.last_alert[key] = now
            return True


async def _investor_scan_loop(client, kis, shutdown_event):
    scan_cooldown = InvestorScanCooldown(cooldown_minutes=60)
    print(f"🔄 BOT 4: Investor scan loop started (interval={INVESTOR_SCAN_INTERVAL}s)")

    while not shutdown_event.is_set():
        try:
            await asyncio.sleep(INVESTOR_SCAN_INTERVAL)
            now = datetime.now()
            hour_min = now.hour * 100 + now.minute
            if hour_min < 910 or hour_min > 1535:
                continue

            codes_to_scan = list(stock_universe.all_codes)[:100]
            if not codes_to_scan:
                continue

            scan_count, alert_count, alerts_batch = 0, 0, []

            for code in codes_to_scan:
                if shutdown_event.is_set():
                    break
                mkt = stock_universe.lookup_market(code)
                name = stock_universe.lookup_name(code) or code

                investor_data = await kis.get_investor_trend(code, market_code=mkt)
                if not investor_data:
                    await asyncio.sleep(0.2)
                    continue

                frgn_amt = investor_data.get("frgn_ntby_tr_pbmn", 0)
                orgn_amt = investor_data.get("orgn_ntby_tr_pbmn", 0)
                frgn_trigger = abs(frgn_amt) >= FRGN_NET_THRESHOLD

                # Always fetch program data (simplified from original double-check)
                program_data = await kis.get_program_trade(code, market_code=mkt)
                prgm_amt = program_data.get("prgm_ntby_tr_pbmn", 0) if program_data else 0
                prgm_trigger = abs(prgm_amt) >= PRGM_NET_THRESHOLD

                if not frgn_trigger and not prgm_trigger:
                    await asyncio.sleep(0.2)
                    scan_count += 1
                    continue

                cooldown_key = f"{code}_frgn" if frgn_trigger else f"{code}_prgm"
                if not await scan_cooldown.try_claim(cooldown_key):
                    await asyncio.sleep(0.2)
                    scan_count += 1
                    continue

                price_info = await kis.get_stock_price(code, market_code=mkt)
                mkt_label = "KOSDAQ" if mkt == "Q" else "KOSPI"
                current_price = price_info.get("price", 0) if price_info else 0
                change_val = _safe_float(price_info.get("change_rate", "0")) if price_info else 0
                direction = "🔺" if change_val > 0 else ("🔻" if change_val < 0 else "▫")

                lines = []
                if frgn_trigger:
                    frgn_dir = "🔵 순매수" if frgn_amt > 0 else "🔴 순매도"
                    lines.extend([
                        f"🌍🔥 **외국인 대량매매 — {name}**", "",
                        f"📌 {name} ({code} | {mkt_label})",
                    ])
                    if current_price:
                        lines.append(f"💰 {current_price:,}원 {direction} {change_val:+.2f}%")
                    lines.append(f"🌍 외국인: {frgn_dir} **{abs(frgn_amt)/100_000_000:.1f}억**")
                    lines.append(f"🏦 기관: {'🔵' if orgn_amt > 0 else '🔴'} {abs(orgn_amt)/100_000_000:.1f}억")

                if prgm_trigger and program_data:
                    prgm_dir = "🔵 순매수" if prgm_amt > 0 else "🔴 순매도"
                    if not frgn_trigger:
                        lines.extend([
                            f"🖥️🔥 **프로그램 대량매매 — {name}**", "",
                            f"📌 {name} ({code} | {mkt_label})",
                        ])
                        if current_price:
                            lines.append(f"💰 {current_price:,}원 {direction} {change_val:+.2f}%")
                    lines.append(f"🖥️ 프로그램: {prgm_dir} **{abs(prgm_amt)/100_000_000:.1f}억**")

                lines.append(f"⏰ {now.strftime('%H:%M')}")
                alerts_batch.append("\n".join(lines))
                alert_count += 1
                scan_count += 1
                await asyncio.sleep(0.3)

            for alert_text in alerts_batch:
                await safe_send(client, INVESTOR_ALERT_CHANNEL, alert_text, link_preview=False)
                await asyncio.sleep(0.5)

            if scan_count > 0:
                print(f"🔍 BOT 4: Scanned {scan_count} stocks, {alert_count} alerts @ {now.strftime('%H:%M')}")

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"❌ BOT 4 scan error: {e}")
            traceback.print_exc()
            await asyncio.sleep(60)

    print("🛑 BOT 4: Investor scan loop stopped")


# ============================================================
# SHARED: Dispatch BOT 2 + BOT 3 (used by both handler + edit_handler)
# ============================================================
def _dispatch_bot2_bot3(client, kis, cooldown, _alert_tasks, chat_name, msg_obj,
                        combined_text, msg_text, ocr_text, is_edit=False):
    """Fire BOT 2 (earnings) and BOT 3 (stock alerts) as background tasks.
    Returns nothing — tasks are fire-and-forget."""
    prefix = "✏️" if is_edit else ""

    # BOT 2: 실적/공시
    if EARNINGS_CHANNEL and combined_text and contains_earnings_keyword(combined_text):
        matched = [kw for kw in _ALL_EARNINGS_LOWER if kw in combined_text.lower()][:5]
        print(f"  {prefix}📈 실적/공시: {matched}")
        task = asyncio.create_task(
            _forward_to_earnings(client, msg_obj, chat_name, matched, combined_text, is_edit=is_edit)
        )
        _alert_tasks.add(task)
        task.add_done_callback(_alert_tasks.discard)

    # BOT 3: 종목 알림
    if kis and combined_text:
        codes = set(stock_universe.find_stocks_in_text(combined_text))
        text_codes = set(stock_universe.find_stocks_in_text(msg_text)) if msg_text else set()
        if codes:
            code_names = [f"{c}({stock_universe.lookup_name(c) or '?'})" for c in codes]
            print(f"  {prefix}🔎 [{chat_name}] 종목 감지: {', '.join(code_names)}")
            task = asyncio.create_task(
                _process_stock_alerts(client, kis, cooldown, codes, text_codes, ocr_text, chat_name)
            )
            _alert_tasks.add(task)
            task.add_done_callback(_alert_tasks.discard)


# ============================================================
# MAIN
# ============================================================
async def main():
    start_time = time.time()
    print("=" * 50)
    print(" Telegram Monitor v8.6 (simplified)")
    print(" BOT1: Filter+Dedup (no crypto) -> @my_filtered_news")
    print(" BOT2: 실적/공시 -> @jason_earnings")
    print(" BOT3: 종목별 시세/거래대금/RISK/MA/외국인/프로그램 + OCR -> @alerts_forme")
    print(" BOT4: 외국인/프로그램 대량매매 스캔 -> investor alert channel")
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
    bot4_ok = kis_ok and bool(INVESTOR_ALERT_CHANNEL)
    if not bot4_ok:
        print("⚠️ INVESTOR_ALERT_CHANNEL not set -- BOT 4 investor scan disabled")

    kis = KISApi() if kis_ok else None
    detector = DuplicateDetector(threshold=SIMILARITY_THRESHOLD)
    cooldown = AlertCooldown(cooldown_minutes=30)

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
    for ch in [TARGET_CHANNEL, EARNINGS_CHANNEL, VOLUME_ALERT_CHANNEL, INVESTOR_ALERT_CHANNEL]:
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
            try: peer_id = utils.get_peer_id(entity)
            except Exception: continue
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
        print(f"🚨 종목 알림 -> {VOLUME_ALERT_CHANNEL}")
    if bot4_ok and kis:
        print(f"🌍 외국인/프로그램 대량매매 스캔 -> {INVESTOR_ALERT_CHANNEL}")
    if OCR_AVAILABLE:
        print("🔍 OCR enabled: image stock detection active")

    bg_tasks = []
    _alert_tasks = set()
    shutdown_event = asyncio.Event()

    def _signal_handler():
        print("🛑 Received shutdown signal...")
        shutdown_event.set()
        asyncio.ensure_future(client.disconnect())

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try: loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError: pass

    async def heartbeat():
        try:
            while not shutdown_event.is_set():
                await asyncio.sleep(600)
                stats = detector.get_stats()
                up = int(time.time() - start_time)
                print(f"💚 Heartbeat: uptime={up//3600}h{(up%3600)//60}m | "
                      f"connected={client.is_connected()} | chats={len(monitored_ids)} | "
                      f"total={stats['total']} unique={stats['unique']} dup={stats['duplicate']} skip={stats['skipped']}")
        except asyncio.CancelledError: pass

    bg_tasks.append(asyncio.create_task(heartbeat()))

    async def periodic_refresh():
        try:
            while not shutdown_event.is_set():
                await asyncio.sleep(1800)
                print("🔄 Refreshing dialog list...")
                added = await refresh_dialogs()
                print(f"  ✅ {added} new, total: {len(monitored_ids)}")
                await stock_universe.ensure_fresh()
        except asyncio.CancelledError: pass

    bg_tasks.append(asyncio.create_task(periodic_refresh()))

    if bot4_ok and kis:
        bg_tasks.append(asyncio.create_task(_investor_scan_loop(client, kis, shutdown_event)))

    # --------------------------------------------------------
    # MESSAGE HANDLER (new messages)
    # --------------------------------------------------------
    @client.on(events.NewMessage())
    async def handler(event):
        try:
            chat = await event.get_chat()
            try: peer_id = utils.get_peer_id(chat)
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

            chat_name = getattr(chat, "title", None) or "Unknown"
            msg = (event.message.text or "").strip()
            has_media = event.message.media is not None

            # OCR
            ocr_text = ""
            if has_media and OCR_AVAILABLE:
                try:
                    ocr_text = await asyncio.wait_for(
                        extract_text_from_image(client, event.message, chat_name=chat_name), timeout=10.0)
                except asyncio.TimeoutError:
                    print(f"  ⚠️ [{chat_name}] OCR timed out (>10s)")
                if has_media and not ocr_text and isinstance(event.message.media, MessageMediaPhoto):
                    print(f"  🔍 [{chat_name}] OCR returned empty for photo")

            combined_text = f"{msg}\n{ocr_text}".strip() if ocr_text else msg

            # BOT 1: Unique messages (no crypto)
            if not msg and not ocr_text:
                if has_media:
                    ok = await safe_forward(client, TARGET_CHANNEL, event.message)
                    if ok:
                        print(f"  📎 [{chat_name}] Media-only forwarded")
                    else:
                        icon = "📺" if isinstance(chat, Channel) and chat.broadcast else "👥"
                        await safe_send(client, TARGET_CHANNEL, f"📎 {icon}**{chat_name}** [미디어 메시지]", link_preview=False)
                        print(f"  📎 [{chat_name}] Media-only sent as text fallback")
                else:
                    print(f"  ⏭️ [{chat_name}] Empty message skipped (no text, no media)")
                return

            if contains_crypto_keyword(combined_text):
                lower = combined_text.lower()
                triggered = [kw for kw in _CRYPTO_LONG if kw in lower]
                if not triggered and _CRYPTO_SHORT_PATTERN:
                    m = _CRYPTO_SHORT_PATTERN.search(lower)
                    if m: triggered = [m.group()]
                print(f"  🚫 [{chat_name}] Crypto filtered (keyword: {triggered[:3]}): {combined_text[:80]}...")
                return

            if await detector.is_duplicate(combined_text):
                print(f"  ♻️ [{chat_name}] Duplicate ({len(combined_text)} chars): {combined_text[:80]}...")
                return

            print(f"📨 [{chat_name}] Unique msg ({len(combined_text)} chars)")
            forwarded = await safe_forward(client, TARGET_CHANNEL, event.message)
            if forwarded:
                print(f"  ✅ Forwarded to {TARGET_CHANNEL}")
            else:
                icon = "📺" if isinstance(chat, Channel) and chat.broadcast else "👥"
                forwarded = await safe_send(client, TARGET_CHANNEL, f"{icon}**{chat_name}**\n{combined_text[:4000]}", link_preview=False)
            if not forwarded:
                print(f"  ❌ FAILED to deliver to {TARGET_CHANNEL}!")

            # BOT 2 + BOT 3 via shared dispatch
            _dispatch_bot2_bot3(client, kis, cooldown, _alert_tasks, chat_name,
                                event.message, combined_text, msg, ocr_text, is_edit=False)

        except errors.FloodWaitError as e:
            lost_preview = ((event.message.text or '')[:60])
            print(f"  ⏳ FloodWait {e.seconds}s — message lost: {lost_preview}...")
            await asyncio.sleep(e.seconds + 1)
        except Exception as e:
            print(f"  ❌ Handler error: {e}")
            traceback.print_exc()

    # --------------------------------------------------------
    # EDIT HANDLER (edited messages — BOT 2 + 3 only, no re-forward to BOT 1)
    # --------------------------------------------------------
    @client.on(events.MessageEdited())
    async def edit_handler(event):
        try:
            chat = await event.get_chat()
            try: peer_id = utils.get_peer_id(chat)
            except Exception: return
            if peer_id in exclude_ids or peer_id not in monitored_ids:
                return
            chat_name = getattr(chat, "title", None) or "Unknown"
            msg = (event.message.text or "").strip()
            if not msg:
                return
            # BOT 2 + BOT 3 via shared dispatch
            _dispatch_bot2_bot3(client, kis, cooldown, _alert_tasks, chat_name,
                                event.message, msg, msg, "", is_edit=True)
        except errors.FloodWaitError as e:
            await asyncio.sleep(e.seconds + 1)
        except Exception as e:
            print(f"  ❌ Edit handler error: {e}")

    print(f"🎧 Listening... (v8.6)")
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
        traceback.print_exc()
        sys.exit(1)
