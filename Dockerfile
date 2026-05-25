FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Playwright/Chromium NOT installed on Render (512MB RAM limit).
# Browser scraping runs from local Mac only.
# On Render, the app uses HTTP-only fetching (Fetcher.get tier).

COPY . .
RUN mkdir -p media static

EXPOSE 8000

CMD ["uvicorn", "webapp:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
