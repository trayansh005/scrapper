# Install system deps required by Playwright
RUN apt-get update && apt-get install -y \
    libnss3 \
    libatk-bridge2.0-0 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpangocairo-1.0-0 \
    libcairo2 \
    libpango-1.0-0 \
    libatk1.0-0 \
    libdrm2 \
    libxshmfence1 \
    ca-certificates \
    fonts-liberation \
    wget \
    nano \
 && rm -rf /var/lib/apt/lists/*

# Install Playwright browsers DURING BUILD
RUN npx playwright install chromium
