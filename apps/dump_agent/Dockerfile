FROM python:3.13-slim AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    libfbclient2 \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir uv
WORKDIR /app
COPY pyproject.toml pyproject.toml
COPY packages/ packages/
COPY apps/dump_agent/ apps/dump_agent/
RUN uv build --wheel --out-dir /wheels packages/cnes_domain && \
    uv build --wheel --out-dir /wheels packages/cnes_infra && \
    uv build --wheel --out-dir /wheels apps/dump_agent && \
    uv pip install --system /wheels/*.whl

FROM python:3.13-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    libfbclient2 \
    && rm -rf /var/lib/apt/lists/*
RUN groupadd -r app && useradd -r -g app app \
    && mkdir -p /var/lib/cnes-agent /app/logs \
    && chown -R app:app /var/lib/cnes-agent /app/logs
ENV DUMP_LOGS_DIR=/app/logs
ENV XDG_STATE_HOME=/var/lib
COPY --from=builder /usr/local/lib/python3.13/site-packages \
     /usr/local/lib/python3.13/site-packages
USER app
ENTRYPOINT ["python", "-m", "dump_agent.main"]
