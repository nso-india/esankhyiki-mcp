# MoSPI MCP Server - Capabilities Report

**Generated:** February 7, 2026  
**Server:** http://127.0.0.1:8000/mcp  
**Source:** [nso-india/esankhyiki-mcp](https://github.com/nso-india/esankhyiki-mcp)

---

## Overview

This MCP server provides AI-ready access to **official Indian government statistics** from the Ministry of Statistics and Programme Implementation (MoSPI). It enables natural language queries for economic, demographic, and social indicators through Claude, ChatGPT, Cursor, or any MCP-compatible client.

---

## Available Datasets

| Dataset | Full Name | Use Case | Status |
|---------|-----------|----------|--------|
| **PLFS** | Periodic Labour Force Survey | Jobs, unemployment, wages, workforce | ✅ Verified |
| **CPI** | Consumer Price Index | Retail inflation, cost of living | ✅ API Working |
| **IIP** | Index of Industrial Production | Industrial growth, manufacturing | ✅ API Working |
| **ASI** | Annual Survey of Industries | Factory performance, employment | ✅ API Working |
| **NAS** | National Accounts Statistics | GDP, economic growth, national income | ✅ Verified |
| **WPI** | Wholesale Price Index | Wholesale inflation, producer prices | ✅ API Working |
| **ENERGY** | Energy Statistics | Energy production, consumption, fuel mix | ✅ Verified |

---

  ## MCP Tools (4-Step Workflow)

  ```
  1_know_about_mospi_api  →  2_get_indicators  →  3_get_metadata  →  4_get_data
  ```

  | Step | Tool | Description |
  |------|------|-------------|
  | 1 | `1_know_about_mospi_api()` | Overview of all 7 datasets. Start here. |
  | 2 | `2_get_indicators(dataset)` | List available indicators for chosen dataset |
  | 3 | `3_get_metadata(dataset, ...)` | Get valid filter values (states, years, categories) |
  | 4 | `4_get_data(dataset, filters)` | Fetch actual data using filter codes from step 3 |

---

## Test Results

### Successfully Retrieved Data

#### 1. PLFS - Unemployment Rate
```json
{
  "year": "2017-18",
  "frequency": "Annually",
  "indicator": "UR (Unemployment Rate, in per cent)",
  "state": "All India",
  "gender": "male",
  "sector": "rural",
  "AgeGroup": "15 years and above",
  "weekly_status": "PS+SS",
  "religion": "Sikhism",
  "socialGroup": "all",
  "General_Education": "all",
  "value": 4.5
}
```
**Records fetched:** 20  
**Filters available:** year, age, education, gender, religion, sector, social_category, state, weekly_status

---

#### 2. NAS - GDP / Gross Value Added
```json
{
  "series": "Current",
  "year": "2025-26",
  "indicator": "Gross Value Added",
  "frequency": "Annual",
  "revision": "First Advance Estimates",
  "industry": "Agriculture, Livestock, Forestry and Fishing",
  "current_price": "5427908",
  "constant_price": "2554071.2073820364"
}
```
**Records fetched:** 10  
**Filters available:** year, approach, revision, industry, subindustry, institutional_sector

---

#### 3. ENERGY - Energy Balance
```json
{
  "year": "2023-24",
  "indicator": "Energy Balance ( in KToE )",
  "use_of_energy_balance": "Supply",
  "energy_commodities": "Coal",
  "end_use_sector": "Exports",
  "value": -1040.87
}
```
**Records fetched:** 10  
**Filters available:** year, energy_commodities, end_use_sector

---

## Dataset Details

### PLFS - Periodic Labour Force Survey
- **8 Annual indicators**, 4 Quarterly, 3 Monthly
- Covers: Labour Force Participation Rate (LFPR), Worker Population Ratio (WPR), Unemployment Rate (UR), wages, worker distribution, employment conditions
- Breakdowns: State, gender, age, education, religion, social category, sector (rural/urban)

### CPI - Consumer Price Index
- Base years: 2010, 2012
- Levels: Group (broad categories) and Item (600+ individual items)
- Covers: Food, fuel, housing, clothing, miscellaneous
- Breakdowns: State, sector, month, year

### IIP - Index of Industrial Production
- Base years: 1993-94, 2004-05, 2011-12
- Frequency: Monthly and Annual
- Categories: Manufacturing, mining, electricity
- Use-based: Basic goods, capital goods, intermediate goods, consumer durables/non-durables

### ASI - Annual Survey of Industries
- **57 indicators** covering factory-sector analytics
- Capital structure: Fixed/working capital, investments
- Production: Output, inputs, value added (GVA)
- Employment: Workers by gender, contract status, mandays
- Wages: Salaries, bonuses, employer contributions
- NIC classification years: 1987, 1998, 2004, 2008

### NAS - National Accounts Statistics
- **22 Annual + 11 Quarterly indicators**
- GDP and GVA (production approach)
- Consumption (private/government)
- Capital formation, trade (exports/imports)
- National income, savings, growth rates
- Series: Current and Back

### WPI - Wholesale Price Index
- **1000+ items** across 5 hierarchical levels
- Major Groups → Groups (22) → Sub-groups (90+) → Sub-sub-groups → Items
- Covers: Primary articles, Fuel & power, Manufactured products, Food index

### ENERGY - Energy Statistics
- 2 indicators: KToE and PetaJoules
- Dimensions: Supply and Consumption
- Commodities: Coal, oil, gas, renewables, electricity
- Tracks: Production, transformation, end-use sectors

---

## Example Queries

| Question | Dataset | Key Parameters |
|----------|---------|----------------|
| "What is India's unemployment rate?" | PLFS | indicator_code=3, frequency_code=1 |
| "Show me CPI inflation trends" | CPI | base_year=2012, level=Group |
| "Industrial production index for 2023" | IIP | base_year=2011-12, frequency=Annually |
| "Factory employment in India" | ASI | classification_year=2008 |
| "India's GDP growth rate" | NAS | indicator_code=1, series=Current |
| "Wholesale price inflation" | WPI | year_code, major_group_code |
| "Energy consumption by sector" | ENERGY | indicator_code=1, use_of_energy_balance_code=1 |

---

## Running the Server

```bash
# Activate environment
source ~/pyenv/esankhyiki-mcp/bin/activate

# Run MCP server (HTTP mode)
python mospi_server.py
# → http://127.0.0.1:8000/mcp

# Or with FastMCP CLI
fastmcp run mospi_server.py:mcp --transport http --port 8000
```

---

## CLI Chat Interface

```bash
python cli_chat.py
```

Commands:
- `1` - List all datasets
- `2 PLFS` - Get indicators
- `3 PLFS indicator_code=3 frequency_code=1` - Get metadata
- `4 PLFS indicator_code=3 frequency_code=1` - Fetch data
- `help` - Show all commands

---

## Summary

| Metric | Value |
|--------|-------|
| Total Datasets | 7 |
| MCP Tools | 4 |
| Datasets with verified data retrieval | 3 (PLFS, NAS, ENERGY) |
| Total sample records fetched | 40 |
| API Endpoint | https://api.mospi.gov.in |

**All 7 datasets have working API connections.** The 4-step workflow (overview → indicators → metadata → data) is functional. Filter parameter names vary by dataset and must be obtained from step 3 (`3_get_metadata`) before fetching data.

---

*Report generated by automated test suite. Source: Ministry of Statistics and Programme Implementation, Government of India.*
