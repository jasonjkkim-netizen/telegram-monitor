"""
Telegram Channel & Group Monitor v4.0
====================================
1) All unique messages → TARGET_CHANNEL (기존)
2) 실적 keyword messages → EARNINGS_CHANNEL
3) 종목 언급 시 거래대금 체크 -> VOLUME_ALERT_CHANNEL

v4.0 Changes:
- Fixed peer ID mismatch (utils.get_peer_id) so all channels are captured
- Dynamic chat enrollment for channels/groups joined after startup
- Added 공시 keywords alongside 실적
- Periodic dialog refresh every 30 minutes
- Improved duplicate detection thresholds
"""

import os
import re
import asyncio
import hashlib
import time
import aiohttp
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from telethon import TelegramClient, events, utils
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Chat

# ============================================================
# CONFIGURATION
# ============================================================
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
TARGET_CHANNEL = os.environ.get("TARGET_CHANNEL", "")
EARNINGS_CHANNEL = os.environ.get("EARNINGS_CHANNEL", "")
VOLUME_ALERT_CHANNEL = os.environ.get("VOLUME_ALERT_CHANNEL", "")
SIMILARITY_THRESHOLD = float(os.environ.get("SIMILARITY_THRESHOLD", "0.85"))

# 한국투자증권 API
KIS_APP_KEY = os.environ.get("KIS_APP_KEY", "")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET", "")
KIS_ACCOUNT_NO = os.environ.get("KIS_ACCOUNT_NO", "")
KIS_ACCOUNT_PROD = os.environ.get("KIS_ACCOUNT_PROD", "01")

# 거래대금 기준 (원 단위)
DAILY_VOLUME_THRESHOLD = int(os.environ.get("DAILY_VOLUME_THRESHOLD", "100000000000"))
FIVE_MIN_THRESHOLD = int(os.environ.get("FIVE_MIN_THRESHOLD", "5000000000"))

KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"

# ============================================================
# 실적 + 공시 KEYWORDS
# ============================================================
EARNINGS_KEYWORDS = [
    "실적", "잠정실적", "실적발표", "실적공시", "실적추정",
    "실적전망", "실적시즌", "실적쇼크", "실적서프라이즈", "어닝쇼크",
    "어닝서프라이즈", "컨센서스",
    "영업이익", "당기순이익", "순이익", "매출액", "매출",
    "영업손실", "순손실", "당기순손실", "적자전환", "흑자전환",
    "적자지속", "흑자지속",
    "분기실적", "1분기", "2분기", "3분기", "4분기",
    "1Q", "2Q", "3Q", "4Q",
    "반기실적", "연간실적",
    "영업이익률", "순이익률", "매출총이익", "EBITDA",
    "EPS", "BPS", "ROE", "ROA", "PER", "PBR",
    "전년대비", "전분기대비", "YoY", "QoQ",
    "잠정치", "확정치", "연결기준", "별도기준",
    "사업보고서", "분기보고서", "반기보고서",
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
    "리가켐바이오": "141080",
    "SK바이오팜": "326030",
    "두산밥캣": "241560",
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
    "엘앤에프": "066970", "L&F": "066970",
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
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()

    async def get_token(self):
        now = time.time()
        if self.access_token and now < self.token_expires - 60:
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
                    print(f"  🔑 KIS 토큰 발급 성공")
                    return self.access_token
                else:
                    print(f"  ❌ KIS 토큰 실패: {data}")
                    return None
        except Exception as e:
            print(f"  ❌ KIS 토큰 에러: {e}")
            return None

    async def get_stock_price(self, stock_code):
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
                        "name": out.get("hts_kor_isnm", ""),
                        "price": int(out.get("stck_prpr", 0)),
                        "change_rate": out.get("prdy_ctrt", "0"),
                        "acml_tr_pbmn": int(out.get("acml_tr_pbmn", 0)),
                        "acml_vol": int(out.get("acml_vol", 0)),
                    }
                else:
                    print(f"  ⚠️ 시세실패 [{stock_code}]: {data.get('msg1','')}")
                    return None
        except Exception as e:
            print(f"  ❌ 시세에러 [{stock_code}]: {e}")
            return None

    async def get_five_min_volume(self, stock_code):
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
                    for item in items[:5]:
                        vol = int(item.get("cntg_vol", 0))
                        price = int(item.get("stck_prpr", 0))
                        total += vol * price
                    return total
                return 0
        except Exception as e:
            print(f"  ❌ 분봉에러 [{stock_code}]: {e}")
            return 0

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
# DUPLICATE DETECTOR (v4.0 - 버그 수정)
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
            k: v for k, v in self.seen_hashes.items()
            if now - v < self.ttl_seconds
        }
        cutoff = max(0, len(self.seen_texts) - self.max_history)
        self.seen_texts = self.seen_texts[cutoff:]
        cleaned = old_count - len(self.seen_hashes)
        if cleaned > 0:
            print(f"  🧹 Cleaned {cleaned} old entries")

    def _get_hash(self, text):
        cleaned = "".join(text.lower().split())
        return hashlib.md5(cleaned.encode()).hexdigest()

    def is_duplicate(self, text):
        self.stats["total"] += 1

        # FIX 1: 빈 메시지만 스킵, 짧은 메시지는 통과시킴
        if not text or text.strip() == "":
            self.stats["skipped"] += 1
            return True

        self._clean_old()
        text_hash = self._get_hash(text)

        # 완전 동일 메시지 체크
        if text_hash in self.seen_hashes:
            self.stats["duplicate"] += 1
            return True

        # 유사도 체크 (짧은 메시지는 유사도 체크 스킵 - 오탐 방지)
        if len(text.strip()) > 50:
            for old_text in self.seen_texts:
                if SequenceMatcher(None, text.lower(), old_text.lower()).ratio() >= self.threshold:
                    self.stats["duplicate"] += 1
                    return True

        # 고유 메시지 등록
        self.seen_hashes[text_hash] = time.time()
        self.seen_texts.append(text)
        self.stats["unique"] += 1
        return False

    def get_stats(self):
        return self.stats


def contains_earnings_keyword(text):
    if not text:
        return False
    tl = text.lower()
    return any(kw.lower() in tl for kw in EARNINGS_KEYWORDS)


# ============================================================
# 알림 쿨다운
# ============================================================
class AlertCooldown:
    def __init__(self, cooldown_minutes=15):
        self.cooldown = cooldown_minutes * 60
        self.last_alert = {}

    def can_alert(self, stock_code):
        now = time.time()
        if now - self.last_alert.get(stock_code, 0) > self.cooldown:
            self.last_alert[stock_code] = now
            return True
        return False

    def reset(self, stock_code):
        self.last_alert.pop(stock_code, None)


# ============================================================
# MAIN
# ============================================================
async def main():
    print("=" * 55)
    print("  Telegram Monitor v4.0")
    print("  + Earnings Filter + Volume Alert")
    print("  + Dynamic chat enrollment")
    print("=" * 55)

    if not all([API_ID, API_HASH, SESSION_STRING, TARGET_CHANNEL]):
        missing = []
        if not API_ID: missing.append("API_ID")
        if not API_HASH: missing.append("API_HASH")
        if not SESSION_STRING: missing.append("SESSION_STRING")
        if not TARGET_CHANNEL: missing.append("TARGET_CHANNEL")
        print(f"\n❌ Missing: {', '.join(missing)}")
        print("Please set these environment variables in Railway")
        return

    kis_ok = all([KIS_APP_KEY, KIS_APP_SECRET, VOLUME_ALERT_CHANNEL])
    if not kis_ok:
        print("\n⚠️ KIS API or VOLUME_ALERT_CHANNEL not set — volume alerts disabled\n")
    if not EARNINGS_CHANNEL:
        print("⚠️ EARNINGS_CHANNEL not set — earnings filter disabled\n")

    kis = KISApi() if kis_ok else None
    detector = DuplicateDetector(threshold=SIMILARITY_THRESHOLD)
    cooldown = AlertCooldown(cooldown_minutes=15)

    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.start()
    me = await client.get_me()
    print(f"\n✅ Connected: {me.first_name} ({me.phone})")

    if kis:
        if await kis.get_token():
            print("✅ KIS API connected")
        else:
            print("⚠️ KIS API failed — volume alerts disabled")
            kis = None

    # Find channels/groups
    print("\n📡 Finding channels & groups...")
    monitored_ids = set()
    ch_count = grp_count = 0

    exclude_ids = set()
    for ch in [TARGET_CHANNEL, EARNINGS_CHANNEL, VOLUME_ALERT_CHANNEL]:
        if ch:
            try:
                ent = await client.get_entity(ch)
                exclude_ids.add(utils.get_peer_id(ent))
                print(f"  ✅ Output channel: {ch} (peer_id: {utils.get_peer_id(ent)})")
            except Exception as e:
                print(f"  ❌ Cannot find channel {ch}: {e}")

    async def refresh_dialogs():
        """Scan dialogs and add new channels/groups to monitored_ids."""
        nonlocal ch_count, grp_count
        added = 0
        async for dialog in client.iter_dialogs():
            e = dialog.entity
            peer_id = utils.get_peer_id(e)
            if peer_id in exclude_ids:
                continue
            if peer_id in monitored_ids:
                continue
            if isinstance(e, Channel) and e.broadcast:
                monitored_ids.add(peer_id)
                print(f"  📺 {dialog.name} (peer_id: {peer_id})")
                ch_count += 1
                added += 1
            elif isinstance(e, Channel) and e.megagroup:
                monitored_ids.add(peer_id)
                print(f"  👥 {dialog.name} (peer_id: {peer_id})")
                grp_count += 1
                added += 1
            elif isinstance(e, Chat):
                monitored_ids.add(peer_id)
                print(f"  👥 {dialog.name} (peer_id: {peer_id})")
                grp_count += 1
                added += 1
        return added

    await refresh_dialogs()

    print(f"\n📊 Monitoring: {ch_count} channels + {grp_count} groups = {len(monitored_ids)} total")
    print(f"📬 All unique -> {TARGET_CHANNEL}")
    if EARNINGS_CHANNEL:
        print(f"📈 실적/공시 -> {EARNINGS_CHANNEL}")
    if kis:
        print(f"💰 거래대금 -> {VOLUME_ALERT_CHANNEL}")
        print(f"   기준: 누적 >= {DAILY_VOLUME_THRESHOLD/1e8:.0f}억 or 5분 >= {FIVE_MIN_THRESHOLD/1e8:.0f}억")

    # 통계 출력 (10분마다)
    async def print_stats():
        while True:
            await asyncio.sleep(600)
            stats = detector.get_stats()
            print(f"\n📊 Stats: total={stats['total']} unique={stats['unique']} dup={stats['duplicate']} skip={stats['skipped']}")
            print(f"   Cache: {len(detector.seen_hashes)} hashes, {len(detector.seen_texts)} texts\n")

    asyncio.create_task(print_stats())

    # Refresh dialogs every 30 minutes to pick up newly joined chats
    async def periodic_refresh():
        while True:
            await asyncio.sleep(1800)
            print("\n🔄 Refreshing dialog list...")
            added = await refresh_dialogs()
            if added:
                print(f"  ✅ Added {added} new chats (total: {len(monitored_ids)})")
            else:
                print(f"  ✅ No new chats (total: {len(monitored_ids)})")

    asyncio.create_task(periodic_refresh())

    # --------------------------------------------------------
    @client.on(events.NewMessage())
    async def handler(event):
        try:
            chat_id = event.chat_id
            # Dynamic enrollment: if chat is not monitored, check if it should be
            if chat_id not in monitored_ids:
                try:
                    chat_entity = await event.get_chat()
                    peer_id = utils.get_peer_id(chat_entity)
                    if peer_id in exclude_ids:
                        return
                    if isinstance(chat_entity, (Channel, Chat)):
                        monitored_ids.add(chat_id)
                        monitored_ids.add(peer_id)
                        chat_name = getattr(chat_entity, "title", "Unknown")
                        print(f"  🆕 Dynamically added: {chat_name} (id: {chat_id}, peer: {peer_id})")
                    else:
                        return
                except Exception:
                    return

            chat = await event.get_chat()
            chat_name = getattr(chat, "title", "Unknown")

            # FIX 2: 미디어 메시지 처리 개선
            msg = event.message.text or ""
            has_media = event.message.media is not None
            is_media_only = (not msg) and has_media

            # 미디어만 있는 메시지는 중복 체크 없이 바로 포워딩
            if is_media_only:
                print(f"📎 [{chat_name}] Media message → forwarding")
                try:
                    await client.forward_messages(TARGET_CHANNEL, event.message)
                    print(f"  ✅ Forwarded media to {TARGET_CHANNEL}")
                except Exception as e:
                    print(f"  ❌ Forward media failed: {e}")
                    try:
                        await client.send_message(TARGET_CHANNEL, f"📎 **{chat_name}**\n\n[미디어 메시지]", link_preview=False)
                    except Exception as e2:
                        print(f"  ❌ Fallback also failed: {e2}")
                return

            # 텍스트 메시지 중복 체크
            if detector.is_duplicate(msg):
                return

            print(f"📨 [{chat_name}] Unique msg ({len(msg)} chars)")

            # 1) Main channel - 포워딩
            forwarded = False
            try:
                await client.forward_messages(TARGET_CHANNEL, event.message)
                forwarded = True
                print(f"  ✅ Forwarded to {TARGET_CHANNEL}")
            except Exception as e:
                print(f"  ⚠️ Forward failed: {e}")
                try:
                    em = "📺" if isinstance(chat, Channel) and chat.broadcast else "👥"
                    await client.send_message(TARGET_CHANNEL, f"{em} **{chat_name}**\n\n{msg}", link_preview=False)
                    forwarded = True
                    print(f"  ✅ Sent as copy to {TARGET_CHANNEL}")
                except Exception as e2:
                    print(f"  ❌ Send also failed: {e2}")

            if not forwarded:
                print(f"  ❌ FAILED to deliver message to {TARGET_CHANNEL}!")

            # 2) Earnings channel
            if EARNINGS_CHANNEL and contains_earnings_keyword(msg):
                matched = [k for k in EARNINGS_KEYWORDS if k.lower() in msg.lower()][:5]
                print(f"  📈 실적/공시: {matched}")
                try:
                    await client.forward_messages(EARNINGS_CHANNEL, event.message)
                    print(f"  ✅ Forwarded to {EARNINGS_CHANNEL}")
                except Exception as e:
                    print(f"  ⚠️ Earnings forward failed: {e}")
                    try:
                        await client.send_message(EARNINGS_CHANNEL, f"📈 **[실적/공시] {chat_name}**\n🔑 {', '.join(matched)}\n\n{msg}", link_preview=False)
                    except Exception as e2:
                        print(f"  ❌ Earnings send failed: {e2}")

            # 3) Volume alert
            if kis and msg:
                codes = extract_stock_codes(msg)
                for code in codes[:3]:
                    if not cooldown.can_alert(code):
                        continue

                    price = await kis.get_stock_price(code)
                    if not price:
                        cooldown.reset(code)
                        continue

                    name = price["name"] or code
                    daily = price["acml_tr_pbmn"]
                    daily_ok = daily >= DAILY_VOLUME_THRESHOLD

                    fivemin = await kis.get_five_min_volume(code)
                    fivemin_ok = fivemin >= FIVE_MIN_THRESHOLD

                    print(f"  💰 [{name}] 누적:{daily/1e8:.0f}억 5분:{fivemin/1e8:.1f}억")

                    if daily_ok or fivemin_ok:
                        reasons = []
                        if daily_ok:
                            reasons.append(f"누적 {daily/1e8:,.0f}억")
                        if fivemin_ok:
                            reasons.append(f"5분 {fivemin/1e8:,.1f}억")

                        alert = (
                            f"🚨 **거래대금 알림**\n\n"
                            f"📌 **{name}** ({code})\n"
                            f"💰 현재가: {price['price']:,}원 ({price['change_rate']}%)\n"
                            f"📊 {' / '.join(reasons)}\n"
                            f"📍 누적거래량: {price['acml_vol']:,}주\n\n"
                            f"💬 출처: {chat_name}\n"
                            f"⏰ {datetime.now().strftime('%H:%M:%S')}"
                        )
                        try:
                            await client.send_message(VOLUME_ALERT_CHANNEL, alert, link_preview=False)
                            print(f"  🚨 ALERT SENT! ✅")
                        except Exception as e:
                            print(f"  ❌ Alert failed: {e}")
                    else:
                        cooldown.reset(code)

                    await asyncio.sleep(0.2)

        except Exception as e:
            print(f"  ❌ Handler error: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n🎧 Listening... (v4.0)\n")
    await client.run_until_disconnected()
    if kis:
        await kis.close()


if __name__ == "__main__":
    asyncio.run(main())
