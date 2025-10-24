# syntax=docker/dockerfile:1

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

WORKDIR /app

# System deps that help build common wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Only copy dependency manifests first for better caching
COPY pyproject.toml requirements.txt* /app/

# Install either from requirements.txt if present or from pyproject
RUN if [ -f "requirements.txt" ]; then \
        pip install -r requirements.txt; \
    else \
        pip install .; \
    fi

RUN apt-get update && apt-get install -y --no-install-recommends sqlite3 \
 && rm -rf /var/lib/apt/lists/*

# Copy the rest of the app
COPY . /app

# Create a nonroot user
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8501

# Streamlit on 0.0.0.0 so it is reachable from outside the container
CMD ["streamlit", "run", "dashboard/streamlit_app.py", "--server.address=0.0.0.0", "--server.port=8501"]
