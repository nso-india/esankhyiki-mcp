# Cross-Dataset Intelligence: 6 Investigations Using India's Open Data

These examples demonstrate what becomes possible when an AI agent can query **official Indian government statistics** through natural language. Each investigation was conducted live against the [MoSPI MCP Server](../README.md) — the same 4-tool workflow, the same public APIs — no pre-processing, no CSVs, no manual data wrangling.

The point isn't just that the data exists. It's that a machine can now **cross-reference datasets, compute derived metrics, and surface paradoxes** that would take a human researcher hours of manual API navigation, code mapping, and spreadsheet work.

---

## How to Read These Examples

Each example follows the same structure:

| Section | What It Contains |
|---------|-----------------|
| **Query** | The natural language question asked |
| **Key Insight** | The surprising or policy-relevant finding (read this first) |
| **Findings** | Tables with the actual data, computed ratios, and breakdowns |
| **How It Was Done** | Which MCP tools were called, which datasets, how many API calls |

The "How It Was Done" section is important — it shows the **mechanical work** the MCP server handles: discovering arbitrary filter codes (e.g., `education_code=6` means "diploma holder"), resolving inconsistent state codes across datasets, and navigating hierarchical commodity structures with 1000+ items.

---

## The Examples

### 1. [The Jobless Growth Detector](example_1_jobless_growth_detector.md)

**Datasets:** NAS + PLFS | **API calls:** 6 | **Data points:** ~30

> Is India's GDP growth actually translating into jobs?

Compares GDP growth rate with unemployment across age groups and sectors. Finds that the "jobless growth" narrative is dead for the overall population (UR halved to 3.2%), but youth unemployment remains 3x higher — and during COVID, unemployment *fell* because workers shifted into disguised employment, not genuine recovery.

---

### 2. [India's Education-Unemployment Paradox](example_2_education_unemployment_paradox.md)

**Dataset:** PLFS | **API calls:** 5 | **Data points:** ~20

> Is education helping Indians get jobs?

The more educated you are, the more likely you are to be unemployed. A postgraduate is 12x more likely to be jobless than an illiterate person. Diploma holders face the worst outcomes at 19.8%. The data reveals voluntary "queue unemployment" — graduates waiting for jobs that match their credentials while the economy produces positions below their expectations.

---

### 3. [Capital vs. Labor in Indian Manufacturing](example_3_capital_vs_labor_manufacturing.md)

**Dataset:** ASI | **API calls:** 5 | **Data points:** 144

> Is automation replacing labor in Indian factories?

Computes derived metrics (fixed capital per worker, GVA per worker) across pharmaceuticals, textiles, and motor vehicles from 2008-2024. Finds that automation is **not** replacing labor — all three industries added workers massively. Motor vehicles is hiring faster than it's adding capital. But textiles, India's biggest manufacturing employer, is also its least productive (4.3x less GVA/worker than pharma).

---

### 4. [Is Food Inflation Squeezing Rural India?](example_4_rural_food_inflation_squeeze.md)

**Dataset:** CPI | **API calls:** 4 | **Data points:** 200+

> Is the cost-of-living crisis hitting rural and urban India differently?

Cross-references food and fuel inflation across 5 major states. Finds that vegetables are running at 20-39% inflation everywhere, but the real story is fuel: **rural and urban fuel prices are moving in opposite directions.** In Bihar, rural fuel costs rise (+0.6%) while urban fuel falls (-4.7%). Rural households rely on kerosene; urban households benefit from falling LPG.

---

### 5. [India's Energy Transition — Reality Check](example_5_energy_transition_reality_check.md)

**Dataset:** ENERGY | **API calls:** 4 | **Data points:** 108

> Is India actually transitioning away from fossil fuels?

Coal's share of primary energy supply *increased* from 56% to 59% over 12 years. Renewables grew 4x (impressive), but their share moved from just 0.9% to 2.2% (not enough). The real transition happening is **oil to coal**, not fossil to renewable. India's energy appetite grew 52% in 12 years — so fast that even rapid renewable additions get diluted.

---

### 6. [The Great Squeeze — Three Paradoxes That Shouldn't Coexist](example_6_the_great_squeeze.md)

**Datasets:** ASI + CPI + WPI + PLFS | **API calls:** 17 | **Data points:** ~200

> Run an anomaly detection loop. Find patterns that are paradoxical and surprising.

The most complex investigation — cross-references **four datasets** to find three converging paradoxes:

1. **Factory profits doubled while real wages per worker fell** — COVID reversed a decade of rising labor share in 2 years. Workers produce 32% more value but take home 2% less in real terms.
2. **Wholesale prices flatlined but retail prices kept climbing** — WPI was near-zero in 2023 but CPI was 5.7%. The 4.8pp gap reveals middleman margin extraction.
3. **20 million women entered rural work** — Female LFPR nearly doubled (23% to 42%), but the surge is almost entirely rural, consistent with distress employment rather than empowerment.

These aren't separate anomalies. They're the same structural shift seen from three angles: value flowing from labor to capital, and from producers to intermediaries.

---

## Complexity Progression

| # | Datasets | Cross-Dataset? | Derived Metrics? | API Calls | Difficulty |
|---|----------|---------------|-----------------|-----------|------------|
| 1 | NAS + PLFS | Yes | No | 6 | Medium |
| 2 | PLFS | No | No | 5 | Simple |
| 3 | ASI | No | Yes (FC/worker, GVA/worker) | 5 | Medium |
| 4 | CPI | No | No | 4 | Simple |
| 5 | ENERGY | No | Yes (% shares) | 4 | Simple |
| 6 | ASI + CPI + WPI + PLFS | **4-way** | Yes (labor share, real wages, WPI-CPI gap) | 17 | **Complex** |

---

## What the MCP Server Handles

These examples look simple on paper — ask a question, get a table. Under the hood, the server is doing work that would otherwise require deep institutional knowledge:

- **Code resolution**: Knowing that `state_code=99` means "All India" in PLFS but `state_code=99` also means "All India" in CPI — with *different state code mappings for individual states* between the two datasets
- **Hierarchical navigation**: WPI has 1000+ items across 5 nesting levels. CPI has groups, subgroups, and 600+ individual items. The metadata step surfaces the right codes.
- **Multi-revision handling**: NAS GDP data returns multiple revisions per year (First Advance, Second Advance, Provisional, Final). The AI selects the latest.
- **Arbitrary filter codes**: There's no way to guess that `education_code=6` means "diploma/certificate" or `nic_id=200821` means "pharmaceuticals" — these must be discovered via `3_get_metadata`.

---

## Reproducing These Examples

All examples were generated using the MoSPI MCP server. To reproduce:

```bash
# 1. Start the server
source ~/pyenv/esankhyiki-mcp/bin/activate
python mospi_server.py
# → http://127.0.0.1:8000/mcp

# 2. Connect any MCP-compatible client (Claude, Cursor, ChatGPT, etc.)
#    Point it at http://127.0.0.1:8000/mcp

# 3. Ask the query from any example — the AI will follow the 4-step workflow automatically
```

Or connect directly to MoSPI's hosted server — see the [main README](../README.md#quick-start) for details.

---

## Data Source

All data is sourced from the **Ministry of Statistics and Programme Implementation (MoSPI)**, Government of India, via their official open APIs at [api.mospi.gov.in](https://api.mospi.gov.in). No data has been modified, interpolated, or supplemented from external sources. Derived metrics (ratios, real wages, percentage shares) are computed from the raw API responses and are noted in each example.

---

*Built with the [MoSPI MCP Server](../README.md) by [Bharat Digital](https://bharatdigital.io) in partnership with the [Data Innovation and Informatics Division (DIID)](https://www.datainnovation.mospi.gov.in/home), MoSPI.*
