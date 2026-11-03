# One image for both NestJS apps; docker-compose selects the entrypoint.
FROM node:22.11-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY tsconfig.json tsconfig.build.json ./
COPY apps ./apps
COPY libs ./libs
COPY infrastructure/elasticsearch ./infrastructure/elasticsearch
RUN npm run build && npm prune --omit=dev

FROM node:22.11-alpine
WORKDIR /app
ENV NODE_ENV=production
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY package.json ./
USER node
