# FastMCP 3.0 Production Dockerfile with OpenTelemetry
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Prevent Python from writing .pyc files and buffering stdout
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# OpenTelemetry configuration defaults (override at runtime)
ENV OTEL_SERVICE_NAME=mospi-mcp-server
ENV OTEL_TRACES_EXPORTER=otlp
ENV OTEL_EXPORTER_OTLP_PROTOCOL=grpc

# Security defaults (override at runtime)
ENV MCP_AUTH_MODE=required
ENV MCP_RATE_LIMIT_PER_MINUTE=120
ENV TELEMETRY_CAPTURE_TOOL_INPUT=false
ENV TELEMETRY_CAPTURE_TOOL_OUTPUT=false
ENV TELEMETRY_LOG_FULL_OUTPUT=false

# Install system dependencies (needed for pandas/numpy/openpyxl)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install OpenTelemetry auto-instrumentation packages
RUN opentelemetry-bootstrap -a install

# Copy the server code, observability module, dataset folder, and swagger specs
COPY mospi_server.py .
COPY observability/ ./observability/
COPY mospi/ ./mospi/
COPY swagger/ ./swagger/

# Expose the port for HTTP transport
EXPOSE 8000

# Run the server with OpenTelemetry instrumentation wrapper
# FastMCP middleware handles IP tracking and input/output capture
CMD ["opentelemetry-instrument", "fastmcp", "run", "mospi_server.py:mcp", "--transport", "http", "--port", "8000", "--host", "0.0.0.0"]
