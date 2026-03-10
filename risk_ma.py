"""
risk_ma.py - Risk level computation and moving average state analysis.

Provides compute_risk_level() and compute_ma_state() for stock alerts.
"""


def compute_risk_level(price_info):
    """Compute risk level based on daily change rate and 52-week position.

    Returns:
        tuple: (risk_label, risk_emoji) e.g. ("\U0001f534 HIGH", "\U0001f534")
    """
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
    """Compute moving average state from a list of daily closing prices.

    Args:
        closes: List of closing prices, most recent first.
        current_price: Optional current price (defaults to closes[0]).

    Returns:
        tuple: (state_label, detail_string)
    """
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

        b, r = signals["bullish"], signals["bearish"]
        if b >= 4:
            state = "\U0001f7e2 \uac15\uc138 (Strong Bullish)"
        elif b > r:
            state = "\U0001f7e2 \uc0c1\uc2b9\ucd94\uc138 (Bullish)"
        elif r >= 4:
            state = "\U0001f534 \uc57d\uc138 (Strong Bearish)"
        elif r > b:
            state = "\U0001f534 \ud558\ub77d\ucd94\uc138 (Bearish)"
        else:
            state = "\U0001f7e1 \ud6a1\ubcf4 (Neutral)"

        ma_detail = " | ".join(parts)
        if cross:
            ma_detail += f" | {cross}"
        return state, ma_detail
    except Exception:
        return "\u2753 \uacc4\uc0b0 \ubd88\uac00", ""
