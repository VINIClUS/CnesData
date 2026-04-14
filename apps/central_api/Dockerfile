FROM python:3.13-slim AS builder
RUN pip install --no-cache-dir uv
WORKDIR /app
COPY pyproject.toml pyproject.toml
COPY packages/ packages/
COPY apps/central_api/ apps/central_api/
RUN uv build --wheel --out-dir /wheels packages/cnes_domain && \
    uv build --wheel --out-dir /wheels packages/cnes_infra && \
    uv build --wheel --out-dir /wheels apps/central_api && \
    uv pip install --system /wheels/*.whl

FROM python:3.13-slim
RUN groupadd -r app && useradd -r -g app app
COPY --from=builder /usr/local/lib/python3.13/site-packages \
     /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin/uvicorn /usr/local/bin/uvicorn
USER app
EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=5s --retries=3 --start-period=10s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/v1/system/health')"
ENTRYPOINT ["uvicorn", "central_api.app:create_app", "--factory", \
            "--host", "0.0.0.0", "--port", "8000"]
