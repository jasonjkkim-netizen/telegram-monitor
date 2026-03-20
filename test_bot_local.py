"""
Comprehensive test suite for bot.py v8.4
Tests all fixed functions without requiring API keys or Telegram connection.
"""
import asyncio
import time
import sys
import os

# Set dummy env vars so module can import without crashing
os.environ.setdefault("API_ID", "12345")
os.environ.setdefault("API_HASH", "test_hash")
os.environ.setdefault("SESSION_STRING", "test_session")
os.environ.setdefault("TARGET_CHANNEL", "@test_channel")

# Import after env vars are set
sys.path.insert(0, os.path.dirname(__file__))
from bot import (
    _safe_float, compute_risk_level, compute_ma_state,
    contains_crypto_keyword, contains_earnings_keyword,
    DuplicateDetector, AlertCooldown, StockUniverse,
    VOLUME_THRESHOLD, _CRYPTO_LONG, _CRYPTO_SHORT_PATTERN,
    STOCK_CODE_PATTERN, _simhash, _hamming_distance,
    InvestorScanCooldown, KISApi,
    FRGN_NET_THRESHOLD, PRGM_NET_THRESHOLD, INVESTOR_SCAN_INTERVAL,
)

PASS = 0
FAIL = 0
TESTS = []

def test(name):
    """Decorator to register and run tests."""
    def decorator(func):
        TESTS.append((name, func))
        return func
    return decorator

def assert_eq(actual, expected, msg=""):
    global PASS, FAIL
    if actual == expected:
        PASS += 1
        return True
    else:
        FAIL += 1
        print(f"    ❌ FAIL: {msg} | expected={expected}, got={actual}")
        return False

def assert_true(val, msg=""):
    global PASS, FAIL
    if val:
        PASS += 1
        return True
    else:
        FAIL += 1
        print(f"    ❌ FAIL: {msg} | expected True, got {val}")
        return False

def assert_false(val, msg=""):
    return assert_true(not val, msg)

# ============================================================
# TEST 1: Module imports
# ============================================================
@test("Module imports and syntax")
def test_imports():
    assert_true(callable(_safe_float), "_safe_float is callable")
    assert_true(callable(compute_risk_level), "compute_risk_level is callable")
    assert_true(callable(compute_ma_state), "compute_ma_state is callable")
    assert_true(callable(contains_crypto_keyword), "contains_crypto_keyword is callable")
    assert_true(callable(contains_earnings_keyword), "contains_earnings_keyword is callable")
    assert_true(hasattr(AlertCooldown, 'mark_alerted'), "AlertCooldown has mark_alerted method")
    assert_true(hasattr(AlertCooldown, 'can_alert'), "AlertCooldown has can_alert method")

# ============================================================
# TEST 2: _safe_float helper
# ============================================================
@test("_safe_float helper function")
def test_safe_float():
    assert_eq(_safe_float("3.14"), 3.14, "normal float string")
    assert_eq(_safe_float("0"), 0.0, "zero string")
    assert_eq(_safe_float("-2.5"), -2.5, "negative float")
    assert_eq(_safe_float("1,234.56"), 1234.56, "comma-separated")
    assert_eq(_safe_float("N/A"), 0.0, "N/A returns default")
    assert_eq(_safe_float(""), 0.0, "empty string returns default")
    assert_eq(_safe_float(None), 0.0, "None returns default")
    assert_eq(_safe_float("abc", 99.0), 99.0, "non-numeric with custom default")
    assert_eq(_safe_float("  5.5  "), 5.5, "whitespace-padded")
    assert_eq(_safe_float("-"), 0.0, "dash returns default")

# ============================================================
# TEST 3: Crypto filter
# ============================================================
@test("Crypto filter and keyword detection")
def test_crypto_filter():
    # Should filter (crypto content)
    assert_true(contains_crypto_keyword("비트코인 가격 급등"), "Korean crypto keyword")
    assert_true(contains_crypto_keyword("Bitcoin price surge"), "English crypto keyword")
    assert_true(contains_crypto_keyword("ETH to the moon"), "Short keyword ETH with boundary")
    assert_true(contains_crypto_keyword("BTC 10만달러 돌파"), "BTC keyword")
    assert_true(contains_crypto_keyword("업비트에서 거래량 폭증"), "Exchange keyword 업비트")

    # Should NOT filter (normal stock news)
    assert_false(contains_crypto_keyword("삼성전자 실적 발표"), "Normal stock news")
    assert_false(contains_crypto_keyword("method acting is great"), "method contains eth but shouldn't trigger")
    assert_false(contains_crypto_keyword("adapted the plan"), "adapted contains ada but shouldn't trigger")
    assert_false(contains_crypto_keyword("dotted line signature"), "dotted contains dot but shouldn't trigger")
    assert_false(contains_crypto_keyword(""), "Empty string")
    assert_false(contains_crypto_keyword(None), "None input")

# ============================================================
# TEST 4: Duplicate detector
# ============================================================
@test("Duplicate detector (SimHash)")
def test_duplicate_detector():
    async def _run():
        det = DuplicateDetector(threshold=0.85, max_history=100, ttl_hours=1)

        # First message should be unique
        assert_false(await det.is_duplicate("삼성전자 3분기 실적 서프라이즈 영업이익 15조"), "First msg is unique")

        # Exact duplicate
        assert_true(await det.is_duplicate("삼성전자 3분기 실적 서프라이즈 영업이익 15조"), "Exact duplicate")

        # Completely different message
        assert_false(await det.is_duplicate("현대차 수소차 신모델 출시 계획 발표"), "Different msg is unique")

        # Empty/whitespace should be skipped (treated as duplicate)
        assert_true(await det.is_duplicate(""), "Empty string is skipped")
        assert_true(await det.is_duplicate("   "), "Whitespace-only is skipped")

        # Check stats
        stats = det.get_stats()
        assert_eq(stats["unique"], 2, "2 unique messages")
        assert_true(stats["duplicate"] >= 1, f"At least 1 duplicate detected (got {stats['duplicate']})")
        assert_true(stats["skipped"] >= 2, "At least 2 skipped (empty + whitespace)")

    asyncio.run(_run())

# ============================================================
# TEST 5: AlertCooldown (fixed logic)
# ============================================================
@test("AlertCooldown with mark_alerted fix")
def test_cooldown():
    cd = AlertCooldown(cooldown_minutes=30)

    # First check: should allow alert
    assert_true(cd.can_alert("005930"), "First alert allowed")

    # can_alert should NOT consume the cooldown (v8.3 fix)
    # So calling can_alert again should still return True (no mark_alerted called yet)
    assert_true(cd.can_alert("005930"), "can_alert doesn't consume cooldown")

    # Now mark as alerted
    cd.mark_alerted("005930")

    # After marking, should NOT allow within cooldown
    assert_false(cd.can_alert("005930"), "Blocked after mark_alerted")

    # Different stock should still be allowed
    assert_true(cd.can_alert("000660"), "Different stock allowed")

    # Reset should clear cooldown
    cd.reset("005930")
    assert_true(cd.can_alert("005930"), "After reset, alert allowed again")

# ============================================================
# TEST 5b: AlertCooldown try_claim (race condition prevention)
# ============================================================
@test("AlertCooldown try_claim (atomic claim)")
def test_cooldown_try_claim():
    async def _run():
        cd = AlertCooldown(cooldown_minutes=30)

        # First claim should succeed
        assert_true(await cd.try_claim("005930"), "First try_claim succeeds")

        # Second claim immediately should FAIL (already claimed)
        assert_false(await cd.try_claim("005930"), "Second try_claim blocked")

        # Different stock should succeed
        assert_true(await cd.try_claim("000660"), "Different stock try_claim succeeds")

        # After reset, can claim again
        cd.reset("005930")
        assert_true(await cd.try_claim("005930"), "After reset, try_claim succeeds again")

    asyncio.run(_run())

# ============================================================
# TEST 6: compute_risk_level with edge cases
# ============================================================
@test("compute_risk_level with edge cases")
def test_risk_level():
    # Normal case
    label, emoji = compute_risk_level({
        "price": 70000, "change_rate": "2.5",
        "w52_hgpr": 80000, "w52_lwpr": 50000,
    })
    assert_true(emoji in ("🟢", "🟡", "🔴", "❓"), f"Valid emoji: {emoji}")

    # High risk: big change + near 52w high
    label, emoji = compute_risk_level({
        "price": 79000, "change_rate": "15.0",
        "w52_hgpr": 80000, "w52_lwpr": 50000,
    })
    assert_eq(emoji, "🔴", "High risk near 52w high with 15% change")

    # Price = 0 (edge case - v8.3 fix)
    label, emoji = compute_risk_level({
        "price": 0, "change_rate": "5.0",
        "w52_hgpr": 80000, "w52_lwpr": 50000,
    })
    assert_eq(emoji, "❓", "Price 0 returns N/A")

    # Negative price (edge case)
    label, emoji = compute_risk_level({
        "price": -100, "change_rate": "1.0",
        "w52_hgpr": 80000, "w52_lwpr": 50000,
    })
    assert_eq(emoji, "❓", "Negative price returns N/A")

    # change_rate = "N/A" (v8.3 fix via _safe_float)
    label, emoji = compute_risk_level({
        "price": 50000, "change_rate": "N/A",
        "w52_hgpr": 80000, "w52_lwpr": 50000,
    })
    assert_true(emoji in ("🟢", "🟡", "🔴"), "N/A change_rate doesn't crash")

    # Missing keys entirely
    label, emoji = compute_risk_level({})
    assert_eq(emoji, "❓", "Empty dict returns N/A")

    # 52w high == 52w low (division by zero protection)
    label, emoji = compute_risk_level({
        "price": 50000, "change_rate": "1.0",
        "w52_hgpr": 50000, "w52_lwpr": 50000,
    })
    assert_true(emoji in ("🟢", "🟡", "🔴"), "Equal w52 doesn't crash")

# ============================================================
# TEST 7: compute_ma_state
# ============================================================
@test("compute_ma_state")
def test_ma_state():
    # Not enough data
    state, detail = compute_ma_state([100, 200])
    assert_true("부족" in state, "Insufficient data detected")

    # Bullish: price above all MAs
    closes = [100] + [90] * 59  # current=100, all MAs ~90
    state, detail = compute_ma_state(closes, current_price=100)
    assert_true("🟢" in state, "Bullish when price above MAs")

    # Bearish: price below all MAs
    closes = [50] + [90] * 59  # current=50, all MAs ~90
    state, detail = compute_ma_state(closes, current_price=50)
    assert_true("🔴" in state, "Bearish when price below MAs")

    # Empty closes
    state, detail = compute_ma_state([])
    assert_true("부족" in state or "불가" in state, "Empty list handled")

# ============================================================
# TEST 8: StockUniverse.find_stocks_in_text
# ============================================================
@test("StockUniverse.find_stocks_in_text")
def test_stock_universe():
    su = StockUniverse()
    # Manually add some stocks
    su.name_to_code = {"삼성전자": "005930", "SK하이닉스": "000660", "현대차": "005380"}
    su.code_to_name = {"005930": "삼성전자", "000660": "SK하이닉스", "005380": "현대차"}
    su.code_to_market = {"005930": "J", "000660": "J", "005380": "J"}
    su.all_codes = {"005930", "000660", "005380"}
    su._build_index()

    # Find by name
    found = su.find_stocks_in_text("삼성전자 실적 발표")
    assert_true("005930" in found, "Found 삼성전자 by name")

    # Find by code
    found = su.find_stocks_in_text("종목코드 005930 확인")
    assert_true("005930" in found, "Found by 6-digit code")

    # Find multiple
    found = su.find_stocks_in_text("삼성전자, SK하이닉스 반도체 실적")
    assert_true(len(found) >= 2, f"Found multiple stocks: {found}")

    # No match
    found = su.find_stocks_in_text("오늘 날씨가 좋습니다")
    assert_eq(len(found), 0, "No stocks in weather text")

    # Empty text
    found = su.find_stocks_in_text("")
    assert_eq(len(found), 0, "Empty text returns empty")

    # 000000 should be excluded
    found = su.find_stocks_in_text("코드 000000 테스트")
    assert_eq(len(found), 0, "000000 excluded")

# ============================================================
# TEST 9: VOLUME_THRESHOLD env var
# ============================================================
@test("VOLUME_THRESHOLD env var connection")
def test_volume_threshold():
    # Default should be 1 billion (1000000000)
    assert_eq(VOLUME_THRESHOLD, 1_000_000_000, "Default VOLUME_THRESHOLD is 1B")

    # Verify it's used in the source code (not hardcoded 5B)
    with open(os.path.join(os.path.dirname(__file__), "bot.py"), "r") as f:
        source = f.read()
    assert_true("< VOLUME_THRESHOLD" in source, "VOLUME_THRESHOLD used in comparison")
    assert_false("< 5_000_000_000" in source, "Hardcoded 5B removed")

# ============================================================
# TEST 10: Integration simulation
# ============================================================
@test("Integration simulation: message flow")
def test_integration():
    """Simulate the message flow without actual Telegram/API connections."""

    # Simulate: normal news → should pass crypto filter → should pass dedup → forward
    msg1 = "삼성전자 3분기 영업이익 15조원 돌파, 시장 기대치 상회"
    assert_false(contains_crypto_keyword(msg1), "Normal news passes crypto filter")

    # Simulate: crypto news → should be filtered
    msg2 = "비트코인 10만달러 돌파, 이더리움도 상승"
    assert_true(contains_crypto_keyword(msg2), "Crypto news filtered")

    # Simulate: earnings news → should match earnings filter
    msg3 = "삼성전자 공시: 3분기 영업이익 15조 3000억원"
    assert_true(contains_earnings_keyword(msg3), "Earnings detected with anchor+data")

    # Simulate: normal news with short crypto-like words → should NOT be filtered
    msg4 = "The adapted method was dotted with improvements"
    assert_false(contains_crypto_keyword(msg4), "English text with eth/ada/dot substrings not filtered")

    # Verify _process_stock_alerts function exists in source
    with open(os.path.join(os.path.dirname(__file__), "bot.py"), "r") as f:
        source = f.read()
    assert_true("async def _process_stock_alerts" in source, "Background task function exists")
    assert_true("asyncio.create_task" in source, "create_task used for BOT 3")

    # Verify handler doesn't have inline stock processing anymore
    handler_section = source.split("async def handler")[1].split("except errors.FloodWaitError")[0]
    assert_false("get_rolling_volume" in handler_section, "No API calls in handler")
    assert_false("get_daily_prices" in handler_section, "No daily price calls in handler")

# ============================================================
# TEST 11: v8.4 - InvestorScanCooldown
# ============================================================
@test("InvestorScanCooldown (v8.4)")
def test_investor_scan_cooldown():
    loop = asyncio.new_event_loop()
    cd = InvestorScanCooldown(cooldown_minutes=1)  # 60s

    # First claim should succeed
    result = loop.run_until_complete(cd.try_claim("005930_frgn"))
    assert_true(result, "First claim succeeds")

    # Second claim same key should fail (within cooldown)
    result = loop.run_until_complete(cd.try_claim("005930_frgn"))
    assert_false(result, "Duplicate claim blocked")

    # Different key should succeed
    result = loop.run_until_complete(cd.try_claim("005930_prgm"))
    assert_true(result, "Different key succeeds")

    # Reset and re-claim should succeed
    cd.reset("005930_frgn")
    result = loop.run_until_complete(cd.try_claim("005930_frgn"))
    assert_true(result, "Claim after reset succeeds")

    loop.close()

# ============================================================
# TEST 12: v8.4 - KISApi has new methods
# ============================================================
@test("KISApi has investor/program methods (v8.4)")
def test_kis_new_methods():
    assert_true(hasattr(KISApi, 'get_investor_trend'), "KISApi has get_investor_trend")
    assert_true(hasattr(KISApi, 'get_program_trade'), "KISApi has get_program_trade")
    assert_true(callable(getattr(KISApi, 'get_investor_trend', None)), "get_investor_trend is callable")
    assert_true(callable(getattr(KISApi, 'get_program_trade', None)), "get_program_trade is callable")

# ============================================================
# TEST 13: v8.4 - Env var defaults
# ============================================================
@test("v8.4 env var defaults")
def test_v84_env_defaults():
    assert_eq(FRGN_NET_THRESHOLD, 5_000_000_000, "FRGN_NET_THRESHOLD default 50억")
    assert_eq(PRGM_NET_THRESHOLD, 3_000_000_000, "PRGM_NET_THRESHOLD default 30억")
    assert_eq(INVESTOR_SCAN_INTERVAL, 900, "INVESTOR_SCAN_INTERVAL default 15분")

# ============================================================
# TEST 14: v8.4 - Source code verification
# ============================================================
@test("v8.4 source code structure")
def test_v84_source():
    with open(os.path.join(os.path.dirname(__file__), "bot.py"), "r") as f:
        source = f.read()

    # BOT 3 should include investor/program data fetch
    process_section = source.split("async def _process_stock_alerts")[1].split("\nexcept")[0]
    assert_true("get_investor_trend" in process_section, "BOT 3 fetches investor trend")
    assert_true("get_program_trade" in process_section, "BOT 3 fetches program trade")
    assert_true("외국인" in process_section, "BOT 3 alert includes 외국인 label")
    assert_true("프로그램" in process_section, "BOT 3 alert includes 프로그램 label")

    # BOT 4 background scanner should exist
    assert_true("async def _investor_scan_loop" in source, "BOT 4 scan loop function exists")
    assert_true("INVESTOR_ALERT_CHANNEL" in source, "INVESTOR_ALERT_CHANNEL env var used")
    assert_true("_investor_scan_loop" in source, "BOT 4 loop referenced in main")

    # API endpoints should be correct
    assert_true("investor-trend-estimate" in source, "investor-trend-estimate endpoint")
    assert_true("HHPTJ04160200" in source, "Correct TR ID for investor trend")
    assert_true("program-trade-by-stock" in source, "program-trade-by-stock endpoint")
    assert_true("FHPPG04650101" in source, "Correct TR ID for program trade")

    # Version bump
    assert_true("v8.4" in source, "Version bumped to v8.4")

# ============================================================
# RUN ALL TESTS
# ============================================================
if __name__ == "__main__":
    print("=" * 60)
    print(" 🧪 Telegram Monitor v8.4 — Test Suite")
    print("=" * 60)
    print()

    for name, func in TESTS:
        print(f"▶ {name}")
        try:
            func()
            print(f"  ✅ Passed")
        except Exception as e:
            FAIL += 1
            print(f"  ❌ EXCEPTION: {e}")
            import traceback
            traceback.print_exc()
        print()

    print("=" * 60)
    total = PASS + FAIL
    print(f" Results: {PASS}/{total} passed, {FAIL} failed")
    if FAIL == 0:
        print(" ✅ ALL TESTS PASSED")
    else:
        print(f" ❌ {FAIL} TEST(S) FAILED")
    print("=" * 60)

    sys.exit(0 if FAIL == 0 else 1)
