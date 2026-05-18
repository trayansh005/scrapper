FROM mcr.microsoft.com/playwright:v1.56.1-jammy

# Install Node.js 20
RUN apt-get update && apt-get install -y curl tmux \
  && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
  && apt-get install -y nodejs \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package*.json ./

ENV NODE_ENV=production

RUN npm ci --omit=dev && npm cache clean --force

COPY . .

# Install chromium only
RUN npx playwright install chromium

# Verify node version
RUN node -v

CMD ["sleep", "infinity"]