FROM python:3.13-slim

# Install Tesseract OCR with Korean language support
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
        tesseract-ocr-kor \
            libleptonica-dev \
                && rm -rf /var/lib/apt/lists/*

                WORKDIR /app

                COPY requirements.txt .
                RUN pip install --no-cache-dir -r requirements.txt

                COPY . .

                CMD ["python", "bot.py"]
