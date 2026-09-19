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

# Build-time frontend config (Vite inlines VITE_* at build). Defaults keep the
# same-origin nginx proxy; deploy workflows pass the per-environment API base.
ARG VITE_API_BASE_URL=/api/v1
ARG VITE_PRECOS_NOINDEX=true
ENV VITE_API_BASE_URL=$VITE_API_BASE_URL
ENV VITE_PRECOS_NOINDEX=$VITE_PRECOS_NOINDEX

RUN bun run codegen && bun run build && bun run bundle:check

# ---------- Stage 2: nginx ----------
FROM nginx:1.29-alpine
# OS_PATCH_LEVEL is referenced in the RUN so its value is part of the layer
# cache key: without it the upgrade freezes at first-build package versions.
ARG OS_PATCH_LEVEL=base
RUN echo "os-patch-level=${OS_PATCH_LEVEL}" && \
    apk upgrade --no-cache && apk add --no-cache gettext
COPY --from=builder /app/dist /usr/share/nginx/html
COPY apps/web_dashboard/nginx/default.conf.template /etc/nginx/templates/default.conf.template
COPY apps/web_dashboard/nginx/security-headers.conf /etc/nginx/templates/security-headers.conf
COPY apps/web_dashboard/nginx/entrypoint.sh /docker-entrypoint.d/40-render-config.sh
RUN chmod +x /docker-entrypoint.d/40-render-config.sh
EXPOSE 80
HEALTHCHECK --interval=30s --timeout=3s CMD wget --quiet --spider http://127.0.0.1/healthz || exit 1
