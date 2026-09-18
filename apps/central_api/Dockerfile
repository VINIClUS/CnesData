FROM python:3.13-slim AS builder
RUN apt-get update && \
    apt-get install --no-install-recommends -y cargo gcc rustc && \
    rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir --only-binary :all: uv==0.9.26
WORKDIR /app
COPY pyproject.toml pyproject.toml
COPY packages/ packages/
COPY apps/central_api/ apps/central_api/
RUN uv build --wheel --out-dir /wheels packages/cnes_contracts && \
    uv build --wheel --out-dir /wheels packages/cnes_domain && \
    uv build --wheel --out-dir /wheels packages/cnes_infra && \
    uv build --wheel --out-dir /wheels apps/central_api && \
    uv pip install --system /wheels/*.whl

FROM python:3.13-slim
RUN groupadd -r app && useradd -r -g app app
COPY --from=builder /usr/local/lib/python3.13/site-packages \
     /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin/uvicorn /usr/local/bin/uvicorn
RUN rm -rf /usr/local/lib/python3.13/site-packages/pip \
           /usr/local/lib/python3.13/site-packages/pip-*.dist-info \
           /usr/local/lib/python3.13/site-packages/uv \
           /usr/local/lib/python3.13/site-packages/uv-*.dist-info \
           /usr/local/lib/python3.13/ensurepip
# OS_PATCH_LEVEL is referenced in the RUN so its value is part of the layer
# cache key: without it the upgrade freezes at first-build package versions.
ARG OS_PATCH_LEVEL=base
RUN echo "os-patch-level=${OS_PATCH_LEVEL}" && \
    apt-get update && \
    apt-get upgrade -y --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*
USER app
EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=5s --retries=3 --start-period=10s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/v1/system/health')"
ENTRYPOINT ["uvicorn", "central_api.app:create_app", "--factory", \
            "--host", "0.0.0.0", "--port", "8000"]
