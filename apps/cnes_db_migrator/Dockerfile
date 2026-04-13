FROM python:3.13-slim AS builder
RUN pip install --no-cache-dir uv
WORKDIR /app
COPY packages/ packages/
COPY apps/cnes_db_migrator/ apps/cnes_db_migrator/
RUN uv pip install --system apps/cnes_db_migrator/

FROM python:3.13-slim
COPY --from=builder /usr/local/lib/python3.13/site-packages \
     /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin/cnes-migrate /usr/local/bin/cnes-migrate
ENTRYPOINT ["cnes-migrate"]
