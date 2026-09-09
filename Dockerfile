# node:22-bookworm — Debian 12 (bullseye is EOL: apt-get faalt op verlopen Release-files). 2026-09-09.
FROM node:22-bookworm

# Install Python 3 + pip via apt
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Node deps (cache layer)
COPY package*.json ./
RUN npm install --omit=dev

# Install Python deps (cache layer)
COPY requirements.txt ./
RUN python3 -m pip install --no-cache-dir --break-system-packages -r requirements.txt

# Copy rest of app (server.js, simulator.py, data/)
COPY . .

ENV PORT=3000
EXPOSE 3000

CMD ["node", "server.js"]
