# Build the React SPA, then serve the static bundle with nginx.
FROM node:20-alpine AS build
WORKDIR /app
COPY package.json ./
RUN npm install
COPY . .
# Base path the SPA is served under. Local compose serves it at the origin root
# (localhost:8088 → BASE_PATH=/); the public deployment serves it under /console
# behind the reverse proxy (the compose override sets BASE_PATH=/console/). Vite
# emits asset URLs, the router basename and the OIDC callback relative to this,
# so it must match how the app is actually reached.
ARG BASE_PATH=/console/
RUN npm run build -- --base="$BASE_PATH"

FROM nginx:alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
# /usr/share/nginx/html/runtime is mounted from the billing-frontend-config
# volume at runtime (config.json written by the Zitadel provisioner).
