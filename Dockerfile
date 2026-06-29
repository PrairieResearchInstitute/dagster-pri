FROM python:3.12-slim

# Install uv (copied from the official uv image)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# System libraries needed at runtime by the geospatial/netcdf wheels
# (rasterio/GDAL, netcdf4, pyogrio, etc.)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libexpat1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies first for better layer caching
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

# Copy the rest of the project and install it
COPY . .
RUN uv sync --frozen

# Put the project's virtualenv on PATH
ENV PATH="/app/.venv/bin:$PATH"
