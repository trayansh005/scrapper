# ✅ Official Playwright image (Ubuntu Jammy)
FROM mcr.microsoft.com/playwright:v1.42.1-jammy

# Set working directory
WORKDIR /app

# Copy backend dependency files
COPY backend/package*.json ./

# Install Node dependencies
RUN npm ci

# Copy backend source code
COPY backend ./

# Run your scraper
CMD ["node", "combined-scraper.js"]
