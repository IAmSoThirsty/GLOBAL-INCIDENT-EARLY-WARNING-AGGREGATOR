# Hardened multi-stage Dockerfile for GIEWA
FROM python:3.11-slim as builder

# Build arguments
ARG BUILD_DATE
ARG VERSION=1.0.0
ARG VCS_REF

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Create build directory
WORKDIR /build

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Runtime stage
FROM python:3.11-slim

# Labels for metadata
LABEL org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.title="GIEWA" \
      org.opencontainers.image.description="Global Incident Early Warning Aggregator" \
      org.opencontainers.image.vendor="Early Warning Systems Team"

# Security: Create non-root user
RUN groupadd -r giewa && useradd -r -g giewa -u 1000 giewa

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy Python packages from builder
COPY --from=builder /root/.local /home/giewa/.local

# Copy application code
COPY --chown=giewa:giewa src/ ./src/
COPY --chown=giewa:giewa config/ ./config/
COPY --chown=giewa:giewa run_server.py .

# Set up Python path
ENV PYTHONPATH=/app
ENV PATH=/home/giewa/.local/bin:$PATH

# Security: Run as non-root user
USER giewa

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/health', timeout=5)"

# Expose port
EXPOSE 8000

# Default command
CMD ["python", "run_server.py"]
