FROM python:3.13-slim AS builder
RUN pip install --no-cache-dir uv
WORKDIR /app
COPY pyproject.toml pyproject.toml
COPY packages/ packages/
COPY apps/data_processor/ apps/data_processor/
RUN uv build --wheel --out-dir /wheels packages/cnes_contracts && \
    uv build --wheel --out-dir /wheels packages/cnes_domain && \
    uv build --wheel --out-dir /wheels packages/cnes_infra && \
    uv build --wheel --out-dir /wheels apps/data_processor && \
    uv pip install --system /wheels/*.whl

FROM python:3.13-slim
RUN apt-get update && \
    apt-get upgrade -y --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*
RUN groupadd -r app && useradd -r -g app app
RUN mkdir -p /logs /data && chown -R app:app /logs /data
COPY --from=builder /usr/local/lib/python3.13/site-packages \
     /usr/local/lib/python3.13/site-packages
RUN rm -rf /usr/local/lib/python3.13/site-packages/pip \
           /usr/local/lib/python3.13/site-packages/pip-*.dist-info \
           /usr/local/lib/python3.13/site-packages/uv \
           /usr/local/lib/python3.13/site-packages/uv-*.dist-info \
           /usr/local/lib/python3.13/ensurepip
USER app
ENTRYPOINT ["python", "-m", "data_processor.main"]
