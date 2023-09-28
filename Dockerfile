FROM node:18.18-bullseye-slim AS builder

RUN mkdir /app
WORKDIR /app

COPY . .
RUN npm ci && npm run build

FROM node:18.18-bullseye-slim

LABEL fly_launch_runtime="nodejs"

WORKDIR /app

COPY --from=builder /app/build ./build
COPY --from=builder /app/package*.json ./

ENV NODE_ENV production

RUN npm ci

CMD [ "npm", "run", "start:prod" ]
