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

FROM python:3.13-slim AS production
ARG AWS_SIGNING_HELPER_VERSION=1.8.5
ARG AWS_SIGNING_HELPER_SHA256=beec9ed1c492d93db809890f16713e3556353294b823c2184ad4e891f1b2b54d
RUN groupadd --system --gid 10001 app && \
    useradd --system --uid 10001 --gid 10001 --no-create-home \
            --home-dir /nonexistent --shell /usr/sbin/nologin app
RUN mkdir -p /opt/aws && \
    release="https://rolesanywhere.amazonaws.com/releases/${AWS_SIGNING_HELPER_VERSION}" && \
    python -c "import sys, urllib.request; urllib.request.urlretrieve(sys.argv[1], sys.argv[2])" \
        "${release}/X86_64/Linux/Amzn2023/aws_signing_helper" /opt/aws/aws_signing_helper && \
    echo "${AWS_SIGNING_HELPER_SHA256}  /opt/aws/aws_signing_helper" \
        | sha256sum --check --strict - && \
    chmod 0555 /opt/aws/aws_signing_helper
COPY --from=builder /usr/local/lib/python3.13/site-packages \
     /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin/uvicorn /usr/local/bin/uvicorn
RUN rm -rf /usr/local/lib/python3.13/site-packages/pip \
           /usr/local/lib/python3.13/site-packages/pip-*.dist-info \
           /usr/local/lib/python3.13/site-packages/uv \
           /usr/local/lib/python3.13/site-packages/uv-*.dist-info \
           /usr/local/lib/python3.13/ensurepip
COPY --chmod=0555 apps/central_api/docker/production-entrypoint.sh \
     /usr/local/bin/production-entrypoint
ARG OS_PATCH_LEVEL=base
RUN echo "os-patch-level=${OS_PATCH_LEVEL}" && \
    apt-get update && \
    apt-get upgrade -y --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*
ENV AWS_PROFILE=cnesdata-production \
    AWS_CONFIG_FILE=/etc/cnesdata/aws/config
USER 10001:10001
EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=15s --retries=3 --start-period=60s \
    CMD python -c "import sys, urllib.request; urllib.request.urlopen(sys.argv[1])" \
        http://localhost:8000/api/v1/system/health
ENTRYPOINT ["/usr/local/bin/production-entrypoint"]

FROM python:3.13-slim
RUN groupadd -r app && useradd -r -g app app
RUN mkdir -p /data && chown app:app /data
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
# timeout/start_period alargados: cold-start do interpretador Python medido em
# até 16s sob carga/hardware modesto (profile local via docker compose).
HEALTHCHECK --interval=15s --timeout=15s --retries=3 --start-period=60s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/v1/system/health')"
ENTRYPOINT ["uvicorn", "central_api.app:create_app", "--factory", \
            "--host", "0.0.0.0", "--port", "8000"]
