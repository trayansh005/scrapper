# 1. Playwright image
FROM mcr.microsoft.com/playwright:v1.56.1-jammy

# 2. Install tmux
RUN apt-get update && apt-get install -y tmux && rm -rf /var/lib/apt/lists/*

# 3. Working directory
WORKDIR /app

# 4. Dependencies
COPY package*.json ./
RUN npm install

# 5. App code
COPY . .

# 6. Keep container alive (important)
CMD ["sleep", "infinity"]
