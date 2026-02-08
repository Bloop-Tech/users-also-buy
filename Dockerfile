# Use the official Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.12-trixie-slim AS builder

# Set working directory
WORKDIR /src


# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen

# Final stage
FROM ghcr.io/astral-sh/uv:python3.12-trixie-slim

# Set working directory
WORKDIR /app

# Copy the virtual environment from builder stage
COPY --from=builder /app/.venv /app/.venv

# Copy application source code
COPY src/ ./src/

# Make sure we use venv
ENV PATH="/app/.venv/bin:$PATH"

# Start the application
CMD ["python", "main.py"]
