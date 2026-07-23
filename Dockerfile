# ============================================================
# Dockerfile — يثبّت Chromium وكل مكتبات النظام التي يحتاجها واتساب
# هذا هو الحل الجذري لخطأ: libglib-2.0.so.0 cannot open shared object file
# ============================================================
FROM node:22-slim

# مكتبات النظام المطلوبة لتشغيل متصفح Chromium (whatsapp-web.js / puppeteer)
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    ca-certificates \
    fonts-liberation \
    fonts-noto-color-emoji \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libx11-6 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libxkbcommon0 \
    libxshmfence1 \
    wget \
  && rm -rf /var/lib/apt/lists/*

# لا تُنزّل puppeteer نسخة Chromium الخاصة به — نستخدم نسخة النظام أعلاه
ENV PUPPETEER_SKIP_DOWNLOAD=true \
    PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true \
    PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium \
    NODE_OPTIONS=--max-old-space-size=512 \
    MALLOC_ARENA_MAX=2 \
    UV_THREADPOOL_SIZE=2 \
    NODE_ENV=production

WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev

COPY . .

CMD ["node", "index.js"]
