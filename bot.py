"""
Telegram Channel & Group Monitor v6.0
======================================
BOT 1: All unique messages (no crypto/coin) -> TARGET_CHANNEL (@my_filtered_news)
BOT 2: 실적/공시 keyword messages -> EARNINGS_CHANNEL (@jason_earnings)
BOT 3: 종목 언급 + 거래대금 >= 10억 실시간 -> VOLUME_ALERT_CHANNEL (@alerts_forme)

v6.0 Fixes:
    - Fixed aiohttp session: removed base_url (not supported in all versions),
      using full URLs instead
    - Fixed KIS API: removed get_realtime_volume (unreliable minute-candle calc),
      now uses acml_tr_pbmn from inquire-price as the volume threshold
    - Fixed BOT 3: earnings keyword check was missing, so volume alerts fired
      on ALL messages mentioning stocks, not just earnings-related ones.
      Now BOT 3 only runs INSIDE the BOT 2 earnings block.
    - Fixed chat_id vs peer_id mismatch: handler now uses peer_id consistently
    - Fixed forward_messages: added FloodWaitError handling with auto retry
    - Fixed asyncio.run() compatibility: wrapped in try/except for event loop
    - Added connection_retries and retry_delay to TelegramClient for stability
    - Added graceful shutdown with background task cancellation
    - Added timeout to aiohttp session to prevent hanging requests
    - Added sys import for sys.exit(1) on fatal error
    - Added telethon.errors import for FloodWaitError handling
    - Removed DAILY_VOLUME_THRESHOLD (unnecessary, simplified to single threshold)
"""

import os
import re
import sys
import asyncio
import hashlib
import time
import aiohttp
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from telethon import TelegramClient, events, utils, errors
from telethon.sessions import StringSession
from telethon.types import Channel, Chat

# ============================================================
# CONFIGURATION
# ============================================================
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
TARGET_CHANNEL = os.environ.get("TARGET_CHANNEL", "")          # @my_filtered_news
EARNINGS_CHANNEL = os.environ.get("EARNINGS_CHANNEL", "")      # @jason_earnings
VOLUME_ALERT_CHANNEL = os.environ.get("VOLUME_ALERT_CHANNEL", "")  # @alerts_forme
SIMILARITY_THRESHOLD = float(os.environ.get("SIMILARITY_THRESHOLD", "0.85"))

# 한국투자증권 API
KIS_APP_KEY = os.environ.get("KIS_APP_KEY", "")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET", "")
KIS_ACCOUNT_NO = os.environ.get("KIS_ACCOUNT_NO", "")
KIS_ACCOUNT_PROD = os.environ.get("KIS_ACCOUNT_PROD", "01")

# 거래대금 기준: BOT 3 실시간 10억원 (1,000,000,000 KRW)
REALTIME_VOLUME_THRESHOLD = int(os.environ.get("REALTIME_VOLUME_THRESHOLD", "1000000000"))

KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"
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
    "비트코인", "이더리움", "가상화폐", "암호화폐", "코인", "토큰",
    "블록체인", "디파이", "엔에프티", "알트코인",
    "업비트", "빗썸", "리플", "솔라나", "도지코인",
    "스테이킹", "채굴", "지갑", "코인마켓", "가상자산",
    "디지털자산", "바이난스", "에어드롭",
]

# ============================================================
# 실적 + 공시 KEYWORDS (BOT 2)
# ============================================================
EARNINGS_KEYWORDS = [
    "실적", "잠정실적", "실적발표", "실적공시", "실적추정", "실적전망",
    "실적시즌", "실적쇼크", "실적서프라이즈",
    "어닝쇼크", "어닝서프라이즈", "컨센서스",
    "영업이익", "당기순이익", "순이익", "매출액", "매출",
    "영업손실", "순손실", "당기순손실",
    "적자전환", "흑자전환", "적자지속", "흑자지속",
    "분기실적", "1분기", "2분기", "3분기", "4분기",
    "1Q", "2Q", "3Q", "4Q",
    "반기실적", "연간실적",
    "영업이익률", "순이익률", "매출총이익",
    "EBITDA", "EPS", "BPS", "ROE", "ROA", "PER", "PBR",
    "전년대비", "전분기대비", "YoY", "QoQ",
    "잠정치", "확정치", "연결기준", "별도기준",
    "사업보고서", "분기보고서", "반기보고서",
    "공시", "공시내용", "수시공시", "주요공시",
    "판매량", "판매실적", "수주", "수주잔고", "수주액",
]
# ============================================================
# 종목명 -> 종목코드 매핑
# ============================================================
STOCK_MAP = {
    "삼성전자": "005930", "삼전": "005930",
    "SK하이닉스": "000660", "하이닉스": "000660", "하닉": "000660",
    "LG에너지솔루션": "373220", "엘지에솔": "373220",
    "삼성바이오로직스": "207940", "삼바": "207940",
    "현대차": "005380", "현대자동차": "005380",
    "기아": "000270", "기아차": "000270",
    "셀트리온": "068270",
    "KB금융": "105560",
    "신한지주": "055550",
    "POSCO홀딩스": "005490", "포스코홀딩스": "005490", "포스코": "005490",
    "NAVER": "035420", "네이버": "035420",
    "카카오": "035720",
    "삼성SDI": "006400",
    "현대모비스": "012330",
    "LG화학": "051910", "엘지화학": "051910",
    "삼성물산": "028260",
    "SK이노베이션": "096770",
    "삼성생명": "032830",
    "하나금융지주": "086790", "하나금융": "086790",
    "우리금융지주": "316140", "우리금융": "316140",
    "LG전자": "066570", "엘지전자": "066570",
    "카카오뱅크": "323410",
    "삼성화재": "000810",
    "KT&G": "033780",
    "HD현대중공업": "329180",
    "삼성전기": "009150",
    "SK텔레콤": "017670", "SKT": "017670",
    "KT": "030200",
    "LG": "003550",
    "SK": "034730",
    "한화에어로스페이스": "012450", "한화에어로": "012450",
    "HD한국조선해양": "009540",
    "두산에너빌리티": "034020",
    "크래프톤": "259960",
    "한국전력": "015760", "한전": "015760",
    "SK스퀘어": "402340",
    "한화오션": "042660",
    "HD현대일렉트릭": "267260",
    "메리츠금융지주": "138040", "메리츠금융": "138040",
    "에코프로비엠": "247540",
    "에코프로": "086520",
    "포스코퓨처엠": "003670",
    "LG이노텍": "011070",
    "한미반도체": "042700",
    "고려아연": "010130",
    "금양": "001570",
    "HLB": "028300",
    "알테오젠": "196170",
    "리가케바이오": "141080",
    "SK바이오팸": "326030",
    "두산밥캿": "241560",
    "CJ제일제당": "097950",
    "아모레퍼시픽": "090430",
    "한화솔루션": "009830",
    "삼성중공업": "010140",
    "대한항공": "003490",
    "현대건설": "000720",
    "미래에셋증권": "006800",
    "한국항공우주": "047810", "KAI": "047810",
    "엔씨소프트": "036570", "엔씨": "036570",
    "넷마블": "251270",
    "펄어비스": "263750",
    "카카오게임즈": "293490",
    "위메이드": "112040",
    "SKC": "011790",
    "SK아이이테크놀로지": "361610", "SKIET": "361610",
    "LG디스플레이": "034220", "LGD": "034220",
    "삼성엔지니어링": "028050",
    "GS건설": "006360",
    "현대제철": "004020",
    "롯데케미칼": "011170",
    "S-Oil": "010950", "에스오일": "010950",
    "한화": "000880",
    "CJ": "001040",
    "GS": "078930",
    "LS": "006260",
    "OCI": "010060",
    "효성": "004800",
    "LS일렉트릭": "010120",
    "두산": "000150",
    "현대글로비스": "086280",
    "이마트": "139480",
    "하이브": "352820", "HYBE": "352820",
    "JYP엔터": "035900", "JYP": "035900",
    "SM": "041510", "에스엠": "041510",
    "유한양행": "000100",
    "한미약품": "128940",
    "종근당": "185750",
    "대웅제약": "069620",
    "SK바이오사이언스": "302440",
    "엘앙에프": "066970", "L&F": "066970",
    "천보": "278280",
    "리노공업": "058470",
    "HPSP": "403870",
    "이오테크닉스": "039030",
    "주성엔지니어링": "036930",
    "원익IPS": "240810",
    "피에스케이": "319660",
}

STOCK_CODE_PATTERN = re.compile(r'\b(\d{6})\b')
# ============================================================
# 한국투자증권 API
# ============================================================
class KISApi:
    def __init__(self):
        self.access_token = None
        self.token_expires = 0
        self.session = None

    async def ensure_session(self):
        """Create aiohttp session WITHOUT base_url for max compatibility."""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=10)
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
                    print(f"🔑 KIS 토큰 발급 성공")
                    return self.access_token
                else:
                    print(f"❌ KIS 토큰 실패: {data}")
                    return None
        except Exception as e:
            print(f"❌ KIS 토큰 에러: {e}")
            return None

    async def get_stock_price(self, stock_code):
        """현재가 + 누적 거래대금 조회 (acml_tr_pbmn = 누적거래대금)"""
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
                        "acml_tr_pbmn": int(out.get("acml_tr_pbmn", "0")),
                        "acml_vol": int(out.get("acml_vol", "0")),
                    }
                else:
                    print(f"⚠️ 시세실패 [{stock_code}]: {data.get('msg1','')}")
                    return None
        except asyncio.TimeoutError:
            print(f"⚠️ 시세 타임아웃 [{stock_code}]")
            return None
        except Exception as e:
            print(f"❌ 시세에러 [{stock_code}]: {e}")
            return None

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
# ============================================================
# 종목 추출
# ============================================================
def extract_stock_codes(text):
    if not text:
        return []
    found = set()
    for m in STOCK_CODE_PATTERN.finditer(text):
        code = m.group(1)
        if code != "000000":
            found.add(code)

    sorted_names = sorted(STOCK_MAP.keys(), key=len, reverse=True)
    for name in sorted_names:
        if name in text or name.upper() in text.upper():
            code = STOCK_MAP[name]
            if code and code != "None":
                found.add(code)

    return list(found)


# ============================================================
# 크립토 필터 (BOT 1)
# ============================================================
def contains_crypto_keyword(text):
    if not text:
        return False
    lower = text.lower()
    return any(kw in lower for kw in CRYPTO_KEYWORDS)


# ============================================================
# 실적/공시 키워드 체크 (BOT 2)
# ============================================================
def contains_earnings_keyword(text):
    if not text:
        return False
    lower = text.lower()
    return any(kw.lower() in lower for kw in EARNINGS_KEYWORDS)


# ============================================================
# DUPLICATE DETECTOR
# ============================================================
class DuplicateDetector:
    def __init__(self, threshold=0.85, max_history=500, ttl_hours=24):
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
            print(f"🧹 Cleaned {cleaned} old entries")

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


# ============================================================
# 알림 쿨다운
# ============================================================
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
# ============================================================
# Safe send/forward with flood wait handling
# ============================================================
async def safe_forward(client, channel, message, max_retries=2):
    for attempt in range(max_retries):
        try:
            await client.forward_messages(channel, message)
            return True
        except errors.FloodWaitError as e:
            wait = e.seconds + 1
            print(f"⚠️ FloodWait: waiting {wait}s...")
            await asyncio.sleep(wait)
        except Exception as e:
            if attempt == 0:
                raise
            print(f"❌ Forward retry failed: {e}")
            return False
    return False


async def safe_send(client, channel, text, max_retries=2, **kwargs):
    for attempt in range(max_retries):
        try:
            await client.send_message(channel, text, **kwargs)
            return True
        except errors.FloodWaitError as e:
            wait = e.seconds + 1
            print(f"⚠️ FloodWait: waiting {wait}s...")
            await asyncio.sleep(wait)
        except Exception as e:
            if attempt == 0:
                raise
            print(f"❌ Send retry failed: {e}")
            return False
    return False
# ============================================================
# MAIN
# ============================================================
async def main():
    print("=" * 60)
    print("  Telegram Monitor v6.0")
    print("  BOT1: Filter+Dedup (no crypto) -> @my_filtered_news")
    print("  BOT2: 실적/공시 -> @jason_earnings")
    print("  BOT3: 거래대금 >= 10억 -> @alerts_forme")
    print("=" * 60)

    if not all([API_ID, API_HASH, SESSION_STRING, TARGET_CHANNEL]):
        missing = []
        if not API_ID:
            missing.append("API_ID")
        if not API_HASH:
            missing.append("API_HASH")
        if not SESSION_STRING:
            missing.append("SESSION_STRING")
        if not TARGET_CHANNEL:
            missing.append("TARGET_CHANNEL")
        print(f"❌ Missing env vars: {', '.join(missing)}")
        return

    kis_ok = all([KIS_APP_KEY, KIS_APP_SECRET, VOLUME_ALERT_CHANNEL])
    if not kis_ok:
        print("⚠️ KIS API or VOLUME_ALERT_CHANNEL not set -- volume alerts disabled")

    if not EARNINGS_CHANNEL:
        print("⚠️ EARNINGS_CHANNEL not set -- earnings filter disabled")

    kis = KISApi() if kis_ok else None
    detector = DuplicateDetector(threshold=SIMILARITY_THRESHOLD)
    cooldown = AlertCooldown(cooldown_minutes=30)

    client = TelegramClient(
        StringSession(SESSION_STRING),
        API_ID,
        API_HASH,
        connection_retries=5,
        retry_delay=2,
    )
    await client.start()

    me = await client.get_me()
    print(f"✅ Connected: {me.first_name} (@{me.username})")

    if kis:
        tok = await kis.get_token()
        if tok:
            print("✅ KIS API connected")
        else:
            print("⚠️ KIS API token failed -- volume alerts disabled")
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
                print(f"  📺 Channel: {entity.title} (id: {peer_id})")
                added += 1
            elif isinstance(entity, Channel) and entity.megagroup:
                monitored_ids.add(peer_id)
                print(f"  👥 Megagroup: {entity.title} (id: {peer_id})")
                added += 1
            elif isinstance(entity, Chat):
                monitored_ids.add(peer_id)
                print(f"  👥 Chat: {entity.title} (id: {peer_id})")
                added += 1
        return added

    await refresh_dialogs()
    print(f"📊 Monitoring {len(monitored_ids)} chats")
    print(f"📬 All unique (no crypto) -> {TARGET_CHANNEL}")
    if EARNINGS_CHANNEL:
        print(f"📈 실적/공시 -> {EARNINGS_CHANNEL}")
    if kis:
        print(f"🚨 거래대금 >= {REALTIME_VOLUME_THRESHOLD/100000000:.0f}억 -> {VOLUME_ALERT_CHANNEL}")

    bg_tasks = []
    async def print_stats():
        try:
            while True:
                await asyncio.sleep(600)
                stats = detector.get_stats()
                print(f"📊 Stats: total={stats['total']} unique={stats['unique']} "
                      f"dup={stats['duplicate']} skip={stats['skipped']}")
                print(f"   Cache: {len(detector.seen_hashes)} hashes, "
                      f"{len(detector.seen_texts)} texts")
        except asyncio.CancelledError:
            pass

    bg_tasks.append(asyncio.create_task(print_stats()))

    async def periodic_refresh():
        try:
            while True:
                await asyncio.sleep(1800)
                print("🔄 Refreshing dialog list...")
                added = await refresh_dialogs()
                if added:
                    print(f"  ✅ Added {added} new chats (total: {len(monitored_ids)})")
                else:
                    print(f"  ✅ No new chats (total: {len(monitored_ids)})")
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

            # Skip our own output channels
            if peer_id in exclude_ids:
                return

            # Dynamic enrollment
            if peer_id not in monitored_ids:
                if isinstance(chat, (Channel, Chat)):
                    monitored_ids.add(peer_id)
                    chat_name = getattr(chat, "title", "Unknown")
                    print(f"  🆕 Dynamically added: {chat_name}")
                else:
                    return

            chat_name = getattr(chat, "title", "Unknown")
            msg = event.message.text
            has_media = event.message.media is not None
            is_media_only = (not msg) and has_media

            # ====== BOT 1: Media-only messages ======
            if is_media_only:
                print(f"📎 [{chat_name}] Media message -> forwarding")
                try:
                    await safe_forward(client, TARGET_CHANNEL, event.message)
                    print(f"  ✅ Forwarded media to {TARGET_CHANNEL}")
                except Exception as e:
                    try:
                        icon = "📺" if isinstance(chat, Channel) and chat.broadcast else "👥"
                        await safe_send(
                            client, TARGET_CHANNEL,
                            f"📎 {icon}**{chat_name}** [미디어 메시지]",
                            link_preview=False,
                        )
                    except Exception as e2:
                        print(f"  ❌ Media fallback failed: {e2}")
                return

            # ====== BOT 1: Text messages ======
            # Filter out crypto/coin messages
            if contains_crypto_keyword(msg):
                return

            # Dedup check
            if detector.is_duplicate(msg):
                return

            print(f"📨 [{chat_name}] Unique msg ({len(msg)} chars)")

            forwarded = False
            try:
                await safe_forward(client, TARGET_CHANNEL, event.message)
                forwarded = True
                print(f"  ✅ Forwarded to {TARGET_CHANNEL}")
            except Exception as e:
                print(f"  ⚠️ Forward failed: {e}")
                try:
                    icon = "📺" if isinstance(chat, Channel) and chat.broadcast else "👥"
                    await safe_send(
                        client, TARGET_CHANNEL,
                        f"{icon}**{chat_name}**\n{msg}",
                        link_preview=False,
                    )
                    forwarded = True
                    print(f"  ✅ Sent as copy to {TARGET_CHANNEL}")
                except Exception as e2:
                    print(f"  ❌ Send also failed: {e2}")

            if not forwarded:
                print(f"  ❌ FAILED to deliver to {TARGET_CHANNEL}!")
            # ====== BOT 2: 실적/공시 channel ======
            if EARNINGS_CHANNEL and contains_earnings_keyword(msg):
                matched = [kw for kw in EARNINGS_KEYWORDS if kw.lower() in msg.lower()][:3]
                print(f"  📈 실적/공시 키워드: {matched}")
                try:
                    await safe_forward(client, EARNINGS_CHANNEL, event.message)
                    print(f"  ✅ Forwarded to {EARNINGS_CHANNEL}")
                except Exception as e:
                    print(f"  ⚠️ Earnings forward failed: {e}")
                    try:
                        await safe_send(
                            client, EARNINGS_CHANNEL,
                            f"📈 **[실적/공시]** {chat_name}\n"
                            f"🔑 키워드: {', '.join(matched)}\n\n{msg[:500]}",
                            link_preview=False,
                        )
                    except Exception as e2:
                        print(f"  ❌ Earnings send failed: {e2}")

                # ====== BOT 3: Volume alert (INSIDE earnings block) ======
                if kis and msg:
                    codes = extract_stock_codes(msg)
                    for code in codes:
                        if not cooldown.can_alert(code):
                            continue

                        price_info = await kis.get_stock_price(code)
                        if not price_info:
                            cooldown.reset(code)
                            continue

                        name = price_info["name"] or code
                        acml = price_info["acml_tr_pbmn"]

                        print(f"  💰 [{code}] {name} 누적거래대금: "
                              f"{acml/100000000:.1f}억")

                        # Threshold: 누적 거래대금 >= 10억원
                        if acml >= REALTIME_VOLUME_THRESHOLD:
                            alert = (
                                f"🚨 **거래대금 매수신호 알림**\n"
                                f"📌 **{name}** ({code})\n"
                                f"💰 현재가: {price_info['price']:,}원 "
                                f"({price_info['change_rate']}%)\n"
                                f"📊 누적거래대금: {acml/100000000:.1f}억\n"
                                f"📍 누적거래량: {price_info['acml_vol']:,}주\n"
                                f"💬 출처: {chat_name}\n"
                                f"⏰ {datetime.now().strftime('%H:%M:%S')}"
                            )
                            try:
                                await safe_send(
                                    client, VOLUME_ALERT_CHANNEL,
                                    alert, link_preview=False,
                                )
                                print(f"  🚨 ALERT SENT for {name} ({code}) ✅")
                            except Exception as e:
                                print(f"  ❌ Alert failed: {e}")
                        else:
                            cooldown.reset(code)

                        await asyncio.sleep(0.2)

        except errors.FloodWaitError as e:
            print(f"  ⚠️ Handler FloodWait: sleeping {e.seconds}s")
            await asyncio.sleep(e.seconds + 1)
        except Exception as e:
            print(f"  ❌ Handler error: {e}")
            import traceback
            traceback.print_exc()
    print(f"🎧 Listening... (v6.0)")

    try:
        await client.run_until_disconnected()
    except KeyboardInterrupt:
        print("\nShutting down...")
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
        print("\nBye!")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
