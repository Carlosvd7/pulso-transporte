FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    PULSO_DATA_DIR=/app/data

RUN pip install --no-cache-dir uv==0.13.10

WORKDIR /app
# Primero solo las dependencias (capa cacheada mientras no cambie uv.lock).
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev --no-install-project
COPY README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev

RUN useradd --create-home --uid 1000 pulso && mkdir -p /app/data && chown pulso /app/data
USER pulso

ENTRYPOINT ["pulso"]
CMD ["--help"]
