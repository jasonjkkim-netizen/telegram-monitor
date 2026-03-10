# telegram-monitor

Real-time Telegram channel and group monitoring bot with Korean stock market integration.

## Overview

This bot monitors all joined Telegram channels and groups, processes incoming messages through three independent pipelines, and routes them to dedicated output channels.

### BOT 1 — Filtered News Feed
Forwards all unique messages (excluding crypto/coin content) to a target channel. Uses SimHash-based near-duplicate detection (85% similarity threshold) to avoid forwarding repeated news.

### BOT 2 — Earnings & Disclosure Filter
Detects Korean stock market earnings reports and corporate disclosures using a two-layer keyword matching system. Messages matching both anchor keywords (e.g., \uacf5\uc2dc, \uc2e4\uc801\ubc1c\ud45c) and data keywords (e.g., \uc601\uc5c5\uc774\uc775, \ub9e4\ucd9c\uc561) are forwarded to the earnings channel.

### BOT 3 — Stock Alert System
When a Korean stock name or 6-digit code is detected (including via OCR on images), the bot queries the Korea Investment & Securities (KIS) API to provide real-time price info, 20-minute rolling trade volume, risk assessment, and moving average analysis.

## Architecture

\`\`\`
bot.py              Main entry point, event handler, Telegram client setup
config.py           Centralized configuration (env vars, keywords, patterns)
filters.py          Crypto filter and earnings keyword filter
dedup.py            SimHash-based duplicate detection
risk_ma.py          Risk level computation and moving average analysis
test_parsing.py     KIS master file parsing validation tests
test_filters.py     Crypto and earnings filter unit tests
test_dedup.py       Duplicate detection unit tests
test_risk_ma.py     Risk/MA computation unit tests
Dockerfile          Container configuration
requirements.txt    Pinned Python dependencies
\`\`\`

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| \`API_ID\` | Yes | Telegram API ID |
| \`API_HASH\` | Yes | Telegram API Hash |
| \`SESSION_STRING\` | Yes | Telethon session string |
| \`TARGET_CHANNEL\` | Yes | Output channel for filtered news (e.g., \`@my_filtered_news\`) |
| \`EARNINGS_CHANNEL\` | No | Output channel for earnings/disclosures |
| \`VOLUME_ALERT_CHANNEL\` | No | Output channel for stock alerts |
| \`KIS_APP_KEY\` | No | Korea Investment & Securities API key |
| \`KIS_APP_SECRET\` | No | Korea Investment & Securities API secret |
| \`SIMILARITY_THRESHOLD\` | No | Duplicate detection threshold (default: 0.85) |
| \`VOLUME_THRESHOLD\` | No | Minimum cumulative trading volume in won (default: 5,000,000,000) |

## Setup

### Prerequisites
- Python 3.10+
- Telegram API credentials (from https://my.telegram.org)
- KIS API credentials (optional, for stock alerts)

### Local Development
\`\`\`bash
pip install -r requirements.txt
export API_ID=your_api_id
export API_HASH=your_api_hash
export SESSION_STRING=your_session_string
export TARGET_CHANNEL=@your_channel
python bot.py
\`\`\`

### Docker
\`\`\`bash
docker build -t telegram-monitor .
docker run -e API_ID=... -e API_HASH=... -e SESSION_STRING=... -e TARGET_CHANNEL=... telegram-monitor
\`\`\`

### Running Tests
\`\`\`bash
# KIS master file parsing tests (requires network)
python test_parsing.py

# Unit tests
python -m pytest test_filters.py test_dedup.py test_risk_ma.py -v
\`\`\`

## Key Features

- **Crypto Filtering**: 50+ keywords in English and Korean to filter out cryptocurrency content
- **Near-Duplicate Detection**: SimHash fingerprinting with configurable Hamming distance threshold
- **OCR Support**: Tesseract-based text extraction from image messages (Korean + English)
- **KIS API Integration**: Real-time stock price, volume, and historical data from Korea Investment & Securities
- **Risk Assessment**: Multi-factor risk scoring based on price volatility and 52-week position
- **Moving Average Analysis**: 5/20/60-day MA with golden/death cross detection
- **Rate Limiting**: Semaphore-based API rate limiting (max 10 concurrent KIS calls)
- **Graceful Shutdown**: Proper SIGTERM/SIGINT handling for Docker/Heroku deployments
- **Auto-Recovery**: Session auto-recovery on connection errors, master file retry with exponential backoff
