FROM mcr.microsoft.com/playwright:v1.56.1-jammy

WORKDIR /app

COPY backend/package*.json ./
RUN npm ci

COPY backend ./

CMD ["node", "combined-scraper.js"]
