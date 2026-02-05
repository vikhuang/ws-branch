# syntax=docker/dockerfile:1
FROM python:3.12-slim AS base

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install dependencies
FROM base AS dependencies
COPY pyproject.toml uv.lock* ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# Production image
FROM base AS production
COPY --from=dependencies /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

COPY . .

# Create non-root user
RUN useradd --create-home appuser && chown -R appuser:appuser /app
USER appuser

CMD ["python", "main.py"]

# Development image
FROM base AS development
COPY --from=dependencies /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

# Install dev dependencies
COPY pyproject.toml uv.lock* ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project

COPY . .

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
