# syntax=docker/dockerfile:1.7

# ---------- Stage 1: build ----------
FROM oven/bun:1.3-alpine AS builder
WORKDIR /app

# Install deps first for cache reuse
COPY apps/web_dashboard/package.json apps/web_dashboard/bun.lock ./
RUN bun install --frozen-lockfile

# Copy source + the OpenAPI contract
COPY apps/web_dashboard/ ./
COPY docs/contracts/openapi.json /openapi/openapi.json

# Override the codegen script's path resolution at build time
ENV OPENAPI_PATH=/openapi/openapi.json

RUN bun run codegen && bun run build && bun run bundle:check

# ---------- Stage 2: nginx ----------
FROM nginx:1.27-alpine
RUN apk add --no-cache gettext
COPY --from=builder /app/dist /usr/share/nginx/html
COPY apps/web_dashboard/nginx/default.conf.template /etc/nginx/templates/default.conf.template
COPY apps/web_dashboard/nginx/entrypoint.sh /docker-entrypoint.d/40-render-config.sh
RUN chmod +x /docker-entrypoint.d/40-render-config.sh
EXPOSE 80
HEALTHCHECK --interval=30s --timeout=3s CMD wget --quiet --spider http://localhost/healthz || exit 1
