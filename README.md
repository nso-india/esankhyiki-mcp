# MoSPI MCP Server

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![FastMCP](https://img.shields.io/badge/FastMCP-3.0-green.svg)](https://gofastmcp.com)

MCP (Model Context Protocol) server for accessing India's Ministry of Statistics and Programme Implementation (MoSPI) data APIs. Built with FastMCP 3.0.

---

## Table of Contents

- [Overview](#overview)
- [Datasets](#datasets)
- [MCP Tools](#mcp-tools)
- [Quick Start](#quick-start)
  - [Installation](#installation)
  - [Running the Server](#running-the-server)
  - [Connecting from an MCP Client](#connecting-from-an-mcp-client)
- [Deployment](#deployment)
  - [Docker](#docker)
  - [Docker Compose](#docker-compose)
  - [FastMCP Cloud](#fastmcp-cloud)
- [Architecture](#architecture)
- [Configuration](#configuration)
- [Contributing](#contributing)
- [Resources](#resources)
- [License](#license)
- [About DIID](#diid)
- [Acknowledgments](#acknowledgments)

---

## Overview

This server provides AI-ready access to official Indian government statistics through the Model Context Protocol (MCP). It acts as a bridge between AI assistants (Claude, ChatGPT, Cursor, etc.) and MoSPI's open data APIs, enabling natural language queries for economic, demographic, and social indicators.

**Key Features:**
- 7 statistical datasets covering employment, inflation, industrial production, GDP, and energy
- Sequential 4-tool workflow designed for LLM consumption
- Swagger-driven parameter validation
- Full OpenTelemetry integration for observability
- Production-ready Docker deployment

If you want to connect your AI agent of choice with the MCP server, you can directly connect it with MOSPI's MCP server. Instructions are available at https://www.datainnovation.mospi.gov.in/mospi-mcp. Instructions to connect ChatGPT or Claude to MCP are available here: https://www.datainnovation.mospi.gov.in/mospi-mcp

---

## Datasets

| Dataset | Full Name | Use For |
|---------|-----------|---------|
| **PLFS** | Periodic Labour Force Survey | Jobs, unemployment, wages, workforce participation |
| **CPI** | Consumer Price Index | Retail inflation, cost of living, commodity prices |
| **IIP** | Index of Industrial Production | Industrial growth, manufacturing output |
| **ASI** | Annual Survey of Industries | Factory performance, industrial employment |
| **NAS** | National Accounts Statistics | GDP, economic growth, national income |
| **WPI** | Wholesale Price Index | Wholesale inflation, producer prices |
| **ENERGY** | Energy Statistics | Energy production, consumption, fuel mix |
<!-- | NMKN | National Namkeen Consumption Index | Bhujia per capita, sev consumption patterns, mixture preference by state | -->

---

## MCP Tools

The server exposes 4 tools that follow a sequential workflow:

```
know_about_mospi_api  →  get_indicators  →  get_metadata  →  get_data
```

| Step | Tool | Description |
|------|------|-------------|
| 1 | `know_about_mospi_api()` | Overview of all datasets. Start here to find the right dataset. |
| 2 | `get_indicators(dataset)` | List available indicators for the chosen dataset. |
| 3 | `get_metadata(dataset, ...)` | Get valid filter values (states, years, categories) and API parameters. |
| 4 | `get_data(dataset, filters)` | Fetch data using filter key-value pairs from metadata. |

**Important:** Tools must be called in order. Skipping `get_metadata` will result in invalid filter codes.

---

## Quick Start

If you want to connect your AI agent of choice with the MCP server, you can directly connect it with MOSPI's MCP server. Instructions are available at https://www.datainnovation.mospi.gov.in/mospi-mcp. Instructions to connect ChatGPT or Claude to MCP are available here: https://www.datainnovation.mospi.gov.in/mospi-mcp

Below instructions are for self-hosting the MCP server. 

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/mospi-mcp-api.git
cd mospi-mcp-api

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Running the Server

```bash
# HTTP transport (remote access)
python mospi_server.py

# OR using FastMCP CLI
fastmcp run mospi_server.py:mcp --transport http --port 8000

# stdio transport (local MCP clients)
fastmcp run mospi_server.py:mcp
```

Server runs at `http://localhost:8000/mcp`

### Connecting from an MCP Client

```python
import asyncio
from fastmcp import Client

async def main():
    async with Client("http://localhost:8000/mcp") as client:
        # Step 1: Get dataset overview
        overview = await client.call_tool("know_about_mospi_api", {})
        print(overview)

        # Step 2: Get indicators for PLFS
        indicators = await client.call_tool("get_indicators", {
            "dataset": "PLFS",
            "user_query": "unemployment rate"
        })
        print(indicators)

asyncio.run(main())
```

---

## Deployment

### Docker

```bash
# Build the image
docker build -t mospi-mcp .

# Run the container
docker run -d -p 8000:8000 --name mospi-server mospi-mcp
```

### Docker Compose

Includes Jaeger for distributed tracing visualization:

```bash
docker-compose up -d
```

Services:
- **MoSPI Server**: http://localhost:8000/mcp
- **Jaeger UI**: http://localhost:16686

### FastMCP Cloud

1. Push code to GitHub
2. Sign in to [FastMCP Cloud](https://fastmcp.cloud)
3. Create project with entrypoint `mospi_server.py:mcp`

---

## Architecture

```
mospi-mcp-api/
├── mospi_server.py          # FastMCP server - tools, validation, routing
├── mospi/
│   └── client.py            # MoSPI API client - HTTP requests to api.mospi.gov.in
├── swagger/                 # Swagger YAML specs per dataset (source of truth for params)
│   └── swagger_user_*.yaml
├── observability/
│   └── telemetry.py         # OpenTelemetry middleware for tracing
├── tests/                   # Per-dataset test files
├── Dockerfile               # Production container with OTEL instrumentation
├── docker-compose.yml       # Full stack with Jaeger
└── requirements.txt
```

### Design Principles

| Principle | Implementation |
|-----------|----------------|
| **Swagger as Source of Truth** | API parameters validated against YAML specs in `swagger/`, not hardcoded |
| **Auto-routing** | CPI routes to Group/Item endpoint based on filters; IIP routes to Annual/Monthly |
| **Validation First** | All filters validated before API calls with clear error messages |
| **LLM-Optimized** | Tool docstrings contain explicit rules and workflow instructions |

---

## Configuration

Environment variables for OpenTelemetry:

| Variable | Description | Default |
|----------|-------------|---------|
| `OTEL_SERVICE_NAME` | Service name in traces | `mospi-mcp-server` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP collector endpoint | `http://localhost:4317` |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | Protocol (`grpc` or `http/protobuf`) | `grpc` |
| `OTEL_TRACES_EXPORTER` | Exporter type (`otlp`, `console`, `none`) | `otlp` |

See `.env.example` for full configuration options.

---

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on:

- Adding new datasets
- Project structure
- Development setup
- Code style

---

## Resources

- [MoSPI Open APIs](https://api.mospi.gov.in) - Official API documentation and e-Sankhyiki portal
- [FastMCP Documentation](https://gofastmcp.com) - MCP framework docs
- [Model Context Protocol](https://modelcontextprotocol.io) - MCP specification

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## DIID

The Data Innovation Lab aims to promote innovation and the use of Information Technology in official statistics, including modernizing survey methods. It seeks to address the current challenges faced by the National Statistical System (NSS). The lab will serve as a platform for testing and developing new ideas through proof-of-concept projects. It will foster collaboration with a wide range of participants such as entrepreneurs, researchers, start-ups, academic institutions, and renowned national and international organizations. By creating an open and dynamic environment, the lab will support the advancement of statistical systems and help improve the quality and efficiency of data collection and analysis.

Know more: https://www.datainnovation.mospi.gov.in/home


## Acknowledgments

Made in partnership with **[Bharat Digital](https://bharatdigital.io)** in pursuit of modernising and humanising how government's use technology in service of the public. 

<!-- Geek spotted! Respect for reading the raw markdown. You're the kind of person India's open data movement needs. -->
