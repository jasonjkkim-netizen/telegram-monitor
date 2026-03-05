FROM python:3.13-slim

# Install Tesseract OCR with Korean language support
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    tesseract-ocr-kor \
    libleptonica-dev \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd --create-home appuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chown -R appuser:appuser /app
USER appuser

# v8.0: Health check - verify Python process is running
HEALTHCHECK --interval=60s --timeout=10s --start-period=40s --retries=3 \
  CMD pgrep -f "python bot.py" > /dev/null || exit 1

CMD ["python", "bot.py"]
