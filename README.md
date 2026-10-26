<!--
  Easter egg: hidden developer credits for the MoSPI eSankhyiki MCP Server.

  Made with love by:
    - Satvik Bajpai
    - Harsh Nisar
    - Mayank Kumawat

  Welcome, traveler. You found the hidden credits. Status: Legendary.
-->
# MoSPI MCP Server

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![FastMCP](https://img.shields.io/badge/FastMCP-3.3-green.svg)](https://gofastmcp.com)
[![MCP Server Tests](https://github.com/nso-india/esankhyiki-mcp/actions/workflows/test.yml/badge.svg)](https://github.com/nso-india/esankhyiki-mcp/actions/workflows/test.yml)

MCP (Model Context Protocol) server for accessing India's Ministry of Statistics and Programme Implementation (MoSPI) data APIs. Built with FastMCP 3.3.

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
- Following statistical datasets covering employment, unemployment, inflation, industrial and services production, GDP and national accounts, energy and renewable energy, higher and school education, gender, health and nutrition, disability, housing and sanitation, household consumption and expenditure, time use, agriculture and livestock, land holdings, debt and investment, AYUSH, environment and climate, banking and financial statistics, economic census, unincorporated enterprises, telecom and digital connectivity.
- Sequential 4-tool workflow designed for LLM consumption
- Swagger-driven parameter validation
- Full OpenTelemetry integration for observability
- Production-ready Docker deployment
- Note: Some NSS rounds include sub-parts (NSS77A, NSS79C, NSS76C, NSS80E), which are shown as separate entries in the README for user understanding. On the server, however, these sub-parts are accessible through their respective parent NSS round   dataset (NSS77, NSS79, NSS76, NSS80) itself, rather than as independent datasets.
---

## Dataset Reference

| Sr. No. | Dataset | Full Name | Use For |
| ------: | :------: | --------- | ------- |
| 1 | **PLFS** | Periodic Labour Force Survey | Employment, unemployment, labour force participation, worker population ratio, wages, employment by industry and occupation |
| 2 | **CPI** | Consumer Price Index | Retail inflation, cost of living, inflation by commodity group, state-wise and rural/urban price indices |
| 3 | **IIP** | Index of Industrial Production | Industrial growth, manufacturing output, mining, electricity generation, sector-wise production indices |
| 4 | **ASI** | Annual Survey of Industries | Factory performance, industrial employment, wages, fixed capital, output, value added, productivity |
| 5 | **NAS** | National Accounts Statistics | GDP, GVA, national income, sector-wise economic growth, savings, capital formation |
| 6 | **WPI** | Wholesale Price Index | Wholesale inflation, producer prices, commodity price indices, inflation trends |
| 7 | **ENERGY** | Energy Statistics | Energy production, consumption, installed capacity, fuel mix, energy intensity, renewable energy statistics |
| 8 | **AISHE** | All India Survey on Higher Education | Universities, colleges, enrolment, teachers, Gross Enrolment Ratio (GER), Gender Parity Index (GPI), higher education infrastructure |
| 9 | **ASUSE** | Annual Survey of Unincorporated Sector Enterprises | Unincorporated enterprises, MSMEs, employment, output, value added, informal sector statistics |
| 10 | **GENDER** | Gender Statistics | Gender indicators, women empowerment, sex ratio, labour participation, education, health, crimes against women |
| 11 | **NFHS** | National Family Health Survey | Fertility, family planning, maternal and child health, nutrition, infant mortality, health indicators |
| 12 | **ENVSTATS** | Environment Statistics | Climate, biodiversity, forests, air and water quality, environmental resources, pollution indicators |
| 13 | **RBI** | RBI Statistics | Banking, money supply, foreign exchange reserves, exchange rates, balance of payments, external sector, financial indicators |
| 14 | **NSS77** | Survey 1 - Situation Assessment of Agricultural Households / Land and Livestock Holdings<br> Survey 2 - All India Debt and Investment Survey (AIDIS) | Survey 1: Land holdings, livestock ownership, crop insurance and farming assets.<br>Survey 2: Household assets, liabilities, debt, investment, borrowing patterns and wealth distribution. |
| 15 | **NSS78** | NSS 78th Round – Multiple Indicator Survey | Drinking water, sanitation, housing amenities, migration, digital connectivity, household living conditions |
| 16 | **CPIALRL** | Consumer Price Index for Agricultural and Rural Labourers | Rural inflation, agricultural labourer cost of living, rural wage index, inflation trends |
| 17 | **HCES** | Household Consumption Expenditure Survey | Household consumption, expenditure patterns, poverty estimation, inequality, consumer behaviour |
| 18 | **TUS** | Time Use Survey | Time allocation, unpaid care work, paid work, household activities, gender time-use patterns |
| 19 | **EC** | Economic Census | Establishments, enterprises, employment, ownership, economic activity, district-wise business statistics |
| 20 | **NSS79** | Survey 1 - Survey on AYUSH<br> Survey 2 - Comprehensive Annual Modular Survey (CAMS) | Survey 1: AYUSH awareness and utilisation, treatment preferences, expenditure on AYUSH services.<br>Survey 2: Education, health expenditure, financial inclusion and digital literacy. |
| 21 | **UDISE** | UDISE+ (Unified District Information System for Education Plus) | Schools, enrolment, dropout, teachers, PTR, GER, NER, GPI, CWSN, ICT facilities, school infrastructure |
| 22 | **MNRE** | Renewable Energy Statistics (Ministry of New and Renewable Energy) | Installed renewable energy capacity, solar, wind, hydro, bioenergy, state-wise renewable energy generation |
| 23 | **NSS76** | Survey 1 - Drinking Water, Sanitation, Hygiene and Housing Conditions<br> Survey 2 - Persons with Disabilities in India | Survey 1: Drinking water sources, water treatment, sanitation, housing characteristics, flood experience.<br>Survey 2: Disability prevalence, education, employment, accessibility, assistive devices, care arrangements. |
| 24 | **NSS75E** | NSS 75th Round – Social Consumption on Education | Literacy, educational attainment, school attendance, education expenditure, internet and computer access, GER/NAR |
| 25 | **NSS80** | Survey 1 - Comprehensive Modular Survey: Telecom (CMST)<br> Survey 2 - Comprehensive Modular Survey: Education (CMSE) | Survey 1: Mobile phone ownership, internet usage, telecom access, digital services, online banking, cyber security awareness.<br>Survey 2: School enrolment, education expenditure, tuition fees, private coaching, scholarships, sources of education funding. |
| 26 | **NSS73** | NSS 73rd Round – Unincorporated Non-Agricultural Enterprises | Enterprise type, enterprise ownership, hired workers, annual emoluments, GVA per worker, employment type, working hours, activity category, sector-wise and state-wise enterprise statistics |
| 27 | **NSS74** | NSS 74th Round – Services Sector Enterprises | Service sector enterprises, registration status, establishments by activity, ICT use, workers per establishment, value per establishment, structural ratios |
| 28 | **NSS71** | Survey 1 - Social Consumption: Education in India<br> Survey 2 - Social Consumption: Health in India | Survey 1: Literacy rate, completed level of education, dropout/never-enrolled, attendance and enrolment status, expenditure per student.<br>Survey 2: Medical expenditure and healthcare, economic independence and mobility, childbirth hospitalisation |
| 29 | **NSS72** | NSS 72nd Round – Household Expenditure on Services and Durable Goods | Household expenditure on transport and miscellaneous consumer services, durable goods acquisition, MPCE, budget share, households having NAE, quintile class of UMPCE |
| 30 | **NSS72T** | NSS 72nd Round – Domestic Tourism Expenditure in India | Overnight and same-day trips, purpose of trip, mode of travel, type of stay, nights spent, tourism expenditure per trip, package vs non-package trips |
| 31 | **ISP** | Index of Service Production | Services sector growth, monthly services output, sub-sector indices |


<!-- | NMKN | National Namkeen Consumption Index | Bhujia per capita, sev consumption patterns, mixture preference by state | -->

---

## MCP Tools

The server exposes 5 tools. Four data tools follow a sequential workflow:

```
list_datasets  ΓåÆ  get_indicators  ΓåÆ  get_metadata  ΓåÆ  get_data
```

| Step | Tool | Description |
|------|------|-------------|
| 1 | `list_datasets()` | Overview of all datasets. Start here to find the right dataset. |
| 2 | `get_indicators(dataset)` | List available indicators for the chosen dataset. |
| 3 | `get_metadata(dataset, ...)` | Get valid filter values (states, years, categories) and API parameters. |
| 4 | `get_data(dataset, filters)` | Fetch data using filter key-value pairs from metadata. |

**Important:** Tools must be called in order. Skipping `get_metadata` will result in invalid filter codes.

`submit_feedback(feedback, tool_name="", dataset="")` reports errors, confusing results,
or improvements to the maintainers. Call it independently of the data workflow.
Include the relevant filters, actual result, and expected result, without secrets,
personal data, or conversation history. Feedback is limited to 5,000 characters.

---

## Quick Start

If you want to connect your AI agent of choice with the MCP server, you can directly connect it with MOSPI's MCP server. Video Guides to connect ChatGPT or Claude to MCP are available here - 


https://github.com/user-attachments/assets/a08376ca-8835-479f-9374-1cca63d5631c



https://github.com/user-attachments/assets/a2d65b8f-c938-43fa-856c-35088063f4bd




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
Γö£ΓöÇΓöÇ mospi_server.py          # FastMCP server - tools, validation, routing
Γö£ΓöÇΓöÇ mospi/
Γöé   ΓööΓöÇΓöÇ client.py            # MoSPI API client - HTTP requests to api.mospi.gov.in
Γö£ΓöÇΓöÇ swagger/                 # Swagger YAML specs per dataset (source of truth for params)
Γöé   ΓööΓöÇΓöÇ swagger_user_*.yaml
Γö£ΓöÇΓöÇ observability/
Γöé   ΓööΓöÇΓöÇ telemetry.py         # OpenTelemetry middleware for tracing
Γö£ΓöÇΓöÇ tests/                   # Pytest suite (covering all datasets)
Γö£ΓöÇΓöÇ Dockerfile               # Production container with OTEL instrumentation
Γö£ΓöÇΓöÇ docker-compose.yml       # Full stack with Jaeger
ΓööΓöÇΓöÇ requirements.txt
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

Runs in-process against the MCP server (no running server needed). Covers all datasets
across the four data tools, plus feedback validation and storage failures using a mocked
Sheets transport. See [CONTRIBUTING.md](CONTRIBUTING.md#testing) for details.

---

## Configuration

### Feedback storage

`submit_feedback` appends one row to a Google Sheet using the
[Sheets append API](https://developers.google.com/workspace/sheets/api/reference/rest/v4/spreadsheets.values/append).
To enable it:

1. Enable the Google Sheets API in your Google Cloud project and create a service account.
2. Create a private Google Sheet with a tab named `Feedback`. Add these headers in A1:E1:
   `feedback_id`, `timestamp_utc`, `tool_name`, `dataset`, `feedback`.
3. Share the sheet with the service account's email as **Editor**.
4. Mount the service account JSON key outside the repository, then set these environment
   variables in the server process. For Docker, pass them into the container and mount
   the key read-only. The server does not automatically load `.env` files.

| Variable | Value |
|----------|-------|
| `FEEDBACK_SPREADSHEET_ID` | The ID between `/d/` and `/edit` in the sheet URL |
| `GOOGLE_APPLICATION_CREDENTIALS` | Absolute path to the service account JSON key |

Only `status: saved` confirms a stored report and returns its `feedback_id`.
Network or API failures return `status: unconfirmed` with an ID for the maintainer
to check before retrying. Text is stored as literal values, including strings starting
with `=`. Without configuration, the tool returns an error and the data tools remain usable.

### OpenTelemetry

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

Made in partnership with **[Bharat Digital](https://bharatdigital.io)** in pursuit of modernising and humanising how governments use technology in service of the public. 

<!-- Geek spotted! Respect for reading the raw markdown. You're the kind of person India's open data movement needs. -->
