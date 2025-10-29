FROM node:18

WORKDIR /app

# Copy package files and install deps (cacheable)
COPY package*.json ./
RUN npm install --legacy-peer-deps

# Copy source
COPY . /app

EXPOSE 3000

# Ensure Vite listens on all interfaces
CMD ["npm", "run", "dev"]
