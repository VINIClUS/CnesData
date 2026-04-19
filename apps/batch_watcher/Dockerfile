FROM python:3.13-slim AS base
WORKDIR /app
RUN pip install --no-cache-dir uv
COPY pyproject.toml uv.lock ./
COPY packages/cnes_domain ./packages/cnes_domain
COPY packages/cnes_infra ./packages/cnes_infra
COPY apps/batch_watcher ./apps/batch_watcher
RUN uv sync --frozen --package batch_watcher
ENTRYPOINT ["uv", "run", "--package", "batch_watcher", "batch-watcher"]
