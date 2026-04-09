# MoSPI MCP Server

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![FastMCP](https://img.shields.io/badge/FastMCP-3.0-green.svg)](https://gofastmcp.com)
[![MCP Server Tests](https://github.com/nso-india/esankhyiki-mcp/actions/workflows/test.yml/badge.svg)](https://github.com/nso-india/esankhyiki-mcp/actions/workflows/test.yml)

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
- [Examples](#examples-what-can-you-do-with-this)
- [Contributing](#contributing)
- [Resources](#resources)
- [License](#license)
- [About DIID](#diid)
- [Acknowledgments](#acknowledgments)

---

## Overview

This server provides AI-ready access to official Indian government statistics through the Model Context Protocol (MCP). It acts as a bridge between AI assistants (Claude, ChatGPT, Cursor, etc.) and MoSPI's open data APIs, enabling natural language queries for economic, demographic, and social indicators.

**Key Features:**
- 19 statistical datasets covering employment, inflation, industrial production, GDP, energy, higher education, gender, health, environment, trade, agriculture, consumption, and economic census
- Sequential 4-tool workflow designed for LLM consumption
- Swagger-driven parameter validation
- Full OpenTelemetry integration for observability
- Production-ready Docker deployment

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
| **AISHE** | All India Survey on Higher Education | Universities, colleges, student enrolment, GER, GPI |
| **ASUSE** | Annual Survey of Unincorporated Enterprises | Informal sector, small businesses, MSME statistics |
| **GENDER** | Gender Statistics | Gender indicators, women empowerment, sex ratio, crimes against women |
| **NFHS** | National Family Health Survey | Fertility, infant mortality, maternal care, nutrition |
| **ENVSTATS** | Environment Statistics | Climate, biodiversity, pollution, water resources, forests |
| **RBI** | RBI Statistics | Foreign trade, forex reserves, exchange rates, balance of payments |
| **NSS77** | NSS 77th Round (Land & Livestock) | Agricultural households, land ownership, farm income, crop insurance |
| **NSS78** | NSS 78th Round (Living Conditions) | Drinking water, sanitation, digital connectivity, migration |
| **CPIALRL** | CPI for Agricultural/Rural Labourers | Rural inflation, agricultural labourer cost of living |
| **HCES** | Household Consumption Expenditure Survey | Consumer spending, poverty analysis, inequality (Gini) |
| **TUS** | Time Use Survey | Time allocation, unpaid work, gender time gaps |
| **EC** | Economic Census | Establishments, enterprises, district-wise business count, workers |
<!-- | NMKN | National Namkeen Consumption Index | Bhujia per capita, sev consumption patterns, mixture preference by state | -->

---

## MCP Tools

The server exposes 4 tools that follow a sequential workflow:

```
list_datasets  →  get_indicators  →  get_metadata  →  get_data
```

| Step | Tool | Description |
|------|------|-------------|
| 1 | `list_datasets()` | Overview of all datasets. Start here to find the right dataset. |
| 2 | `get_indicators(dataset)` | List available indicators for the chosen dataset. |
| 3 | `get_metadata(dataset, ...)` | Get valid filter values (states, years, categories) and API parameters. |
| 4 | `get_data(dataset, filters)` | Fetch data using filter key-value pairs from metadata. |

**Important:** Tools must be called in order. Skipping `get_metadata` will result in invalid filter codes.

---

## Quick Start

If you want to connect your AI agent of choice with the MCP server, you can directly connect it with MOSPI's MCP server. Video Guides to connect ChatGPT or Claude to MCP are available here - 

https://github.com/user-attachments/assets/ec23db03-c5ad-4bdd-af3a-9387bd906b3c

https://github.com/user-attachments/assets/4d2adb2a-a350-4563-8408-c0790bb94412

To get more information, visit - https://www.datainnovation.mospi.gov.in/mospi-mcp

The instructions below are for self-hosting the MCP server.

### Installation

```bash
# Clone the repository
git clone https://github.com/nso-india/esankhyiki-mcp.git
cd esankhyiki-mcp

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

### Connecting from CLI Tools

**Server URL:** `https://mcp.mospi.gov.in/`

#### Claude Code

```bash
claude mcp add esankhyiki-mcp --transport http https://mcp.mospi.gov.in/
```

Verify with `claude mcp list`.

#### Cursor / Windsurf

Add to `.cursor/mcp.json` or `.windsurf/mcp.json`:

```json
{
  "mcpServers": {
    "esankhyiki-mcp": {
      "command": "npx",
      "args": ["mcp-remote", "https://mcp.mospi.gov.in/"]
    }
  }
}
```

#### Antigravity

Add to your Antigravity MCP settings:

```json
{
  "mcpServers": {
    "mospi_api": {
      "serverUrl": "https://mcp.mospi.gov.in/"
    }
  }
}
```

#### Verify Connection

```bash
curl -s -X POST https://mcp.mospi.gov.in/ \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"0.1"}}}'
```

A successful response returns `serverInfo` with `"name": "MoSPI Data Server"`.

#### Local Server

If running locally:
```bash
claude mcp add esankhyiki-mcp --transport http http://localhost:8000/mcp
```

Or with the FastMCP Python client:
```python
import asyncio
from fastmcp import Client

async def main():
    async with Client("http://localhost:8000/mcp") as client:
        overview = await client.call_tool("list_datasets", {})
        print(overview)

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
├── tests/                   # Pytest suite (62 tests across all 19 datasets)
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
| **LLM-Optimized** | Tool docstrings document parameters, return values, and workflow sequence |

---

## Testing

```bash
pip install -r tests/requirements-test.txt
pytest tests/ -v -p no:anyio
```

Runs in-process against the MCP server (no running server needed). Covers all 19 datasets across all 4 tools. See [CONTRIBUTING.md](CONTRIBUTING.md#testing) for details.

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

## Examples: What Can You Do With This?

We ran 6 real investigations using the MCP server — from simple single-dataset queries to a 4-dataset anomaly detection loop. Each example includes the exact query, findings, and a breakdown of the API calls made.

| # | Investigation | Datasets | Finding |
|---|--------------|----------|---------|
| 1 | [Jobless Growth Detector](examples/example_1_jobless_growth_detector.md) | NAS + PLFS | Youth unemployment is 3x overall rate even in 9% GDP growth years |
| 2 | [Education-Unemployment Paradox](examples/example_2_education_unemployment_paradox.md) | PLFS | Postgraduates are 12x more likely to be unemployed than illiterates |
| 3 | [Capital vs. Labor in Manufacturing](examples/example_3_capital_vs_labor_manufacturing.md) | ASI | Automation isn't replacing labor — all 3 industries added workers massively |
| 4 | [Rural Food Inflation Squeeze](examples/example_4_rural_food_inflation_squeeze.md) | CPI | Rural and urban fuel prices are moving in opposite directions |
| 5 | [Energy Transition Reality Check](examples/example_5_energy_transition_reality_check.md) | ENERGY | Coal's share *increased* from 56% to 59% over 12 years |
| 6 | [The Great Squeeze](examples/example_6_the_great_squeeze.md) | ASI + CPI + WPI + PLFS | Factory profits doubled while real wages fell — 3 paradoxes converge |

See the [examples README](examples/README.md) for the full guide, complexity progression, and how to reproduce.

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

Made in partnership with **[Bharat Digital](https://bharatdigital.io)** in pursuit of modernising and humanising how governments use technology in service of the public. 

<!-- Geek spotted! Respect for reading the raw markdown. You're the kind of person India's open data movement needs. -->
