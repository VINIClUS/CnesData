FROM python:3.13-slim AS builder
RUN pip install --no-cache-dir --only-binary :all: uv==0.9.26
WORKDIR /app
COPY pyproject.toml pyproject.toml
COPY packages/ packages/
COPY apps/ apps/
RUN for package in packages/*; do uv build --wheel --out-dir /wheels "$package" || exit 1; done && \
    uv build --wheel --out-dir /wheels apps/data_processor && \
    uv pip install --system \
        /wheels/cnes_contracts-*.whl \
        /wheels/cnes_domain-*.whl \
        /wheels/cnes_data-*.whl \
        /wheels/cnes_governance-*.whl \
        /wheels/cnes_control_plane-*.whl \
        /wheels/cnes_observability-*.whl \
        /wheels/cnes_runtime-*.whl \
        /wheels/data_processor-*.whl

FROM python:3.13-slim
RUN groupadd -r app && useradd -r -g app app
RUN mkdir -p /logs /data && chown -R app:app /logs /data
COPY --from=builder /usr/local/lib/python3.13/site-packages \
     /usr/local/lib/python3.13/site-packages
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
# Fargate bind mounts are root:root 0755 unless the image declares VOLUME at the path.
VOLUME ["/tmp", "/logs", "/data"]
USER app
ENTRYPOINT ["python", "-m", "data_processor.main"]
