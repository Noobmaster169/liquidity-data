FROM node:24-slim
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY src ./src
COPY config ./config
COPY idl ./idl
VOLUME ["/app/data", "/app/state"]
ENTRYPOINT ["node", "src/cli.ts"]
