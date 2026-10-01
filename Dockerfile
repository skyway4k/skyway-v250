FROM node:20-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY package.json .
RUN npm install --omit=dev
COPY server.js .
COPY public ./public
COPY data ./data
ENV NODE_ENV=production
CMD ["node", "server.js"]
