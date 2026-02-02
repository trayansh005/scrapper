# 1. Use the official Playwright image (comes with browsers pre-installed)
FROM mcr.microsoft.com/playwright:v1.56.1-jammy

# 2. Set the working directory
WORKDIR /app

# 3. Copy package files first (to cache npm install)
COPY package*.json ./
RUN npm install

# 4. Copy the rest of your scraper code
COPY . .

# 5. Start your scraper
# Replace 'npm start' with whatever command runs your script
CMD ["npm", "start"]