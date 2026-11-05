# Сборка React/Vite TWA и раздача статики через nginx на порту 80
FROM node:20-alpine AS build
ENV NODE_OPTIONS=--max-old-space-size=512
WORKDIR /app
COPY package*.json ./
RUN npm install --no-audit --no-fund
COPY . .
RUN npm run build

FROM nginx:alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
