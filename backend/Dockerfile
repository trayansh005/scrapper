# Use the latest stable Playwright image (v1.58.2)
FROM mcr.microsoft.com/playwright:v1.58.2-jammy

# Install tmux for your manual sessions
RUN apt-get update && apt-get install -y tmux && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies
COPY package*.json ./
RUN npm install

# Copy code
COPY . .

# Keep it alive so you can 'exec' into it and run your commands
CMD ["sleep", "infinity"]