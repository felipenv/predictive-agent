# -------- builder: heavy but disposable --------
FROM python:3.11 AS builder
WORKDIR /app

# Build toolchains only in the builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libffi-dev libssl-dev pkg-config rustc cargo git ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

# uv + stick to Python 3.11
RUN pip install --no-cache-dir uv
ENV UV_PYTHON=/usr/local/bin/python3.11

# Prefer wheels globally; forbid sdists for the packages that kept compiling on arm64
ENV PIP_PREFER_BINARY=1
ENV PIP_ONLY_BINARY=pyarrow,orjson,rpds-py

# Copy just the dependency files first to maximize Docker layer caching
COPY pyproject.toml uv.lock ./
# Use the lockfile exactly; add a simple retry for flaky networks
RUN uv sync --frozen || (sleep 2 && uv sync --frozen)

# Now bring in your code
COPY src/ ./src/

# -------- runtime: slim and clean --------
FROM python:3.11-slim
WORKDIR /app

# Runtime-only deps (git for GitPython; ca-certs for HTTPS)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

# Copy prebuilt venv + app from builder
COPY --from=builder /app /app

# Use the prebuilt venv directly
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Hints for GitPython (safe even if not strictly needed)
ENV GIT_PYTHON_GIT_EXECUTABLE=/usr/bin/git \
    GIT_PYTHON_REFRESH=warn

# Non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser \
 && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000
CMD ["python", "-m", "src.mcp"]
