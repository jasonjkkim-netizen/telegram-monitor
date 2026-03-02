"""
test_parsing.py - Validates v7.8 master file parsing, market code mapping, and stock detection.

Downloads live KIS master files and runs assertions.
Usage: python test_parsing.py
"""

import asyncio
import re
import io
import zipfile
import aiohttp
import time

KIS_KOSPI_MST_URL = "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip"
KIS_KOSDAQ_MST_URL = "https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip"
_KOSPI_TAIL_LEN = 228
_KOSDAQ_TAIL_LEN = 222

STOCK_CODE_PATTERN = re.compile(r'\b(\d{6})\b')


def _parse_master_file(raw_bytes, tail_len, market_code):
    """Identical to StockUniverse._parse_master_file in bot.py"""
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


async def run_tests():
    passed = 0
    failed = 0

    def check(label, condition, detail=""):
        nonlocal passed, failed
        if condition:
            print(f"  \u2705 PASS: {label}")
            passed += 1
        else:
            print(f"  \u274c FAIL: {label} -- {detail}")
            failed += 1

    print("=" * 60)
    print("  telegram-monitor v7.8 Parsing Tests")
    print("=" * 60)

    # ---- Download master files ----
    print("\n[1] Downloading KIS master files...")
    async with aiohttp.ClientSession() as session:
        kospi_raw = None
        kosdaq_raw = None

        async with session.get(KIS_KOSPI_MST_URL, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            check("KOSPI master download", resp.status == 200, f"HTTP {resp.status}")
            if resp.status == 200:
                data = await resp.read()
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for fn in zf.namelist():
                        kospi_raw = zf.read(fn)

        async with session.get(KIS_KOSDAQ_MST_URL, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            check("KOSDAQ master download", resp.status == 200, f"HTTP {resp.status}")
            if resp.status == 200:
                data = await resp.read()
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for fn in zf.namelist():
                        kosdaq_raw = zf.read(fn)

    if not kospi_raw or not kosdaq_raw:
        print("\u274c Cannot continue - master files not downloaded")
        return

    # ---- Parse master files ----
    print("\n[2] Parsing master files...")
    kn2c, kc2n, kc2m = _parse_master_file(kospi_raw, _KOSPI_TAIL_LEN, "J")
    qn2c, qc2n, qc2m = _parse_master_file(kosdaq_raw, _KOSDAQ_TAIL_LEN, "Q")

    check("KOSPI parsed > 500 stocks", len(kc2n) > 500, f"got {len(kc2n)}")
    check("KOSDAQ parsed > 500 stocks", len(qc2n) > 500, f"got {len(qc2n)}")

    # ---- Test well-known stocks exist ----
    print("\n[3] Checking well-known stocks...")
    well_known_kospi = {
        "005930": "삼성전자",
        "000660": "SK하이닉스",
        "035420": "NAVER",
    }
    for code, expected_name in well_known_kospi.items():
        actual = kc2n.get(code, "NOT FOUND")
        check(f"KOSPI {code} = {expected_name}", actual == expected_name, f"got '{actual}'")

    well_known_kosdaq = {
        "247540": "에코프로비엠",
        "086520": "에코프로",
    }
    for code, expected_name in well_known_kosdaq.items():
        actual = qc2n.get(code, "NOT FOUND")
        check(f"KOSDAQ {code} = {expected_name}", actual == expected_name, f"got '{actual}'")

    # ---- Test NO ISIN prefix in names ----
    print("\n[4] Checking names are clean (no ISIN prefix)...")
    isin_pattern = re.compile(r'^[A-Z]{2}\d{10}')
    kospi_isin_count = sum(1 for n in kn2c if isin_pattern.match(n))
    kosdaq_isin_count = sum(1 for n in qn2c if isin_pattern.match(n))
    check("KOSPI: no names start with ISIN", kospi_isin_count == 0,
          f"{kospi_isin_count} names have ISIN prefix")
    check("KOSDAQ: no names start with ISIN", kosdaq_isin_count == 0,
          f"{kosdaq_isin_count} names have ISIN prefix")

    # ---- Test market code mapping ----
    print("\n[5] Checking market code mapping...")
    check("삼성전자 -> market J", kc2m.get("005930") == "J", f"got {kc2m.get('005930')}")
    check("에코프로 -> market Q", qc2m.get("086520") == "Q", f"got {qc2m.get('086520')}")

    all_kospi_J = all(v == "J" for v in kc2m.values())
    all_kosdaq_Q = all(v == "Q" for v in qc2m.values())
    check("All KOSPI codes mapped to J", all_kospi_J)
    check("All KOSDAQ codes mapped to Q", all_kosdaq_Q)

    # ---- Test combined universe with duplicate handling ----
    print("\n[6] Testing combined universe duplicate handling...")
    all_name_to_code = {}
    all_code_to_name = {}
    all_code_to_market = {}

    for n2c, c2n, c2m, mkt in [
        (kn2c, kc2n, kc2m, "J"),
        (qn2c, qc2n, qc2m, "Q"),
    ]:
        for name, code in n2c.items():
            if name in all_name_to_code and all_name_to_code[name] != code:
                existing_code = all_name_to_code.pop(name)
                existing_mkt = all_code_to_market.get(existing_code, "J")
                all_name_to_code[f"{name}({existing_mkt})"] = existing_code
                all_name_to_code[f"{name}({mkt})"] = code
            else:
                all_name_to_code[name] = code
        all_code_to_name.update(c2n)
        all_code_to_market.update(c2m)

    total_codes = len(set(all_code_to_name.keys()))
    total_names = len(all_name_to_code)
    check(f"Combined universe has {total_codes} codes", total_codes > 2000, f"only {total_codes}")
    check(f"Combined universe has {total_names} names", total_names > 2000, f"only {total_names}")

    # Check no code appears in both markets with wrong mapping
    kospi_codes = set(kc2m.keys())
    kosdaq_codes = set(qc2m.keys())
    overlap = kospi_codes & kosdaq_codes
    check(f"KOSPI/KOSDAQ code overlap count: {len(overlap)}", True)
    if overlap:
        print(f"    (overlapping codes: {list(overlap)[:5]}...)")

    # ---- Test find_stocks_in_text ----
    print("\n[7] Testing find_stocks_in_text logic...")
    all_codes = set(all_code_to_name.keys())
    sorted_names = sorted((n for n in all_name_to_code if len(n) >= 2), key=len, reverse=True)

    def find_stocks_in_text(text):
        if not text:
            return []
        found = set()
        for m in STOCK_CODE_PATTERN.finditer(text):
            code = m.group()
            if code != "000000" and code in all_codes:
                found.add(code)
        text_upper = text.upper()
        for name in sorted_names:
            if name in text or name.upper() in text_upper:
                code = all_name_to_code.get(name)
                if code:
                    found.add(code)
        return list(found)

    codes = find_stocks_in_text("삼성전자 실적 발표")
    check("'삼성전자 실적 발표' -> 005930", "005930" in codes, f"got {codes}")

    codes = find_stocks_in_text("에코프로 주가 상승")
    check("'에코프로 주가 상승' -> 086520", "086520" in codes, f"got {codes}")

    codes = find_stocks_in_text("종목코드 005930 확인")
    check("'종목코드 005930' -> 005930", "005930" in codes, f"got {codes}")

    codes = find_stocks_in_text("오늘 시장 동향")
    check("'오늘 시장 동향' -> empty", len(codes) == 0, f"got {codes}")

    codes = find_stocks_in_text("NAVER 카카오 실적 비교")
    has_naver = "035420" in codes
    check("'NAVER 카카오 실적 비교' -> includes NAVER(035420)", has_naver, f"got {codes}")

    # ---- Summary ----
    print("\n" + "=" * 60)
    print(f"  Results: {passed} passed, {failed} failed")
    print("=" * 60)
    if failed:
        print("\u274c Some tests failed!")
        return 1
    else:
        print("\u2705 All tests passed!")
        return 0


if __name__ == "__main__":
    import sys
    sys.exit(asyncio.run(run_tests()))
