FROM node:20.18-bullseye-slim AS builder

RUN mkdir /app
WORKDIR /app

COPY . .
RUN npm ci && npm run build

FROM node:20.18-bullseye-slim

WORKDIR /app

COPY --from=builder /app/dist ./dist
COPY --from=builder /app/package*.json ./

ENV NODE_ENV production

RUN npm ci

CMD [ "npm", "run", "start:server" ]
