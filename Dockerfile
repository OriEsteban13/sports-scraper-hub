FROM python:3.12-slim

# System dependencies required by Playwright/Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget curl ca-certificates \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
    libgbm1 libasound2 libpango-1.0-0 libcairo2 \
    fonts-liberation libappindicator3-1 xdg-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browser (patchright/Chromium used by Scrapling)
RUN python -m playwright install chromium
RUN python -m playwright install-deps chromium

# Copy application code
COPY . .

# Create media directory (used as local fallback when Supabase is not configured)
RUN mkdir -p media

EXPOSE 8000

CMD ["uvicorn", "webapp:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
