FROM mcr.microsoft.com/playwright:v1.56.1-jammy

RUN apt-get update \
 && apt-get install -y tmux \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package*.json ./

ENV NODE_ENV=production
RUN npm ci --omit=dev && npm cache clean --force

COPY . .

# install only chromium if that's what you use
RUN npx playwright install chromium

CMD ["sleep", "infinity"]