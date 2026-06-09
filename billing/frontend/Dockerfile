# Build the React SPA, then serve the static bundle with nginx.
FROM node:20-alpine AS build
WORKDIR /app
COPY package.json ./
RUN npm install
COPY . .
RUN npm run build

FROM nginx:alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
# /usr/share/nginx/html/runtime is mounted from the billing-frontend-config
# volume at runtime (config.json written by the Zitadel provisioner).
