# UK Healthcare Big Data Pipeline
# Single-stage image — uv manages the Python environment inside the container.
#
# Build:
#   docker build -t uk-healthcare-pipeline .
#
# Run (Marimo notebook UI at http://localhost:2718):
#   docker run -p 2718:2718 -v "$(pwd)/lake:/app/lake" uk-healthcare-pipeline
#
# Run (Prefect UI at http://localhost:4200 — needs two terminals):
#   docker run -p 4200:4200 -v "$(pwd)/lake:/app/lake" \
#     uk-healthcare-pipeline uv run prefect server start --host 0.0.0.0
#   docker run --network host -v "$(pwd)/lake:/app/lake" \
#     uk-healthcare-pipeline uv run python flows/pipeline_flow.py
#
# Run tests inside Docker:
#   docker run uk-healthcare-pipeline uv run pytest tests/ -v -m "not slow"
#
# Why mount lake/ as a volume?
#   The pipeline writes Bronze files to lake/ at runtime. Mounting it means:
#   - Data persists after the container stops
#   - You can inspect lake/ from the host machine
#   - Multiple containers can share the same lake (e.g. Prefect + Marimo)

FROM python:3.11-slim

# Copy uv binary from the official image — pinned to a minor version for reproducibility
COPY --from=ghcr.io/astral-sh/uv:0.6 /uv /usr/local/bin/uv

# Set working directory
WORKDIR /app

# Copy dependency specification first so Docker can cache this layer.
# The layer is only invalidated when pyproject.toml or uv.lock changes,
# not when source code changes — keeping rebuilds fast.
COPY pyproject.toml uv.lock ./

# Install all dependencies into .venv (including dev deps for tests)
# --frozen: use the exact versions in uv.lock (reproducible builds)
# --no-cache: keep the image small
# --all-groups: include the dev dependency group (pytest, ruff, mypy)
RUN uv sync --frozen --no-cache --all-groups

# Copy application source
COPY src/       src/
COPY flows/     flows/
COPY notebooks/ notebooks/
COPY tests/     tests/

# Runtime directories — populated when the pipeline runs.
# These are intentionally empty; mount lake/ as a volume to persist data.
RUN mkdir -p lake outputs

# Run as non-root for security (required by Kubernetes PodSecurityPolicy)
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Expose ports:
#   2718 — Marimo notebook UI
#   4200 — Prefect orchestration UI
EXPOSE 2718 4200

# Health check: verify the pipeline package is importable
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD uv run python -c "import pipeline" || exit 1

# Default command: open the introduction notebook.
# Override with any command, e.g.:
#   docker run ... uk-healthcare-pipeline uv run python flows/pipeline_flow.py
CMD ["uv", "run", "marimo", "edit", \
     "--host", "0.0.0.0", \
     "--port", "2718", \
     "notebooks/00_introduction.py"]
