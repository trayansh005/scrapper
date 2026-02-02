FROM mcr.microsoft.com/playwright:v1.56.1-jammy

# 🔑 CRITICAL: tell Playwright where browsers are
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

WORKDIR /app

COPY backend/package*.json ./
RUN npm ci

COPY backend ./

CMD ["node", "combined-scraper.js"]
