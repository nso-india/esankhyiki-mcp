# Example 4: Is Food Inflation Squeezing Rural India?

## Query

> "Compare CPI inflation for Food & Beverages vs Fuel & Light between rural and urban sectors across Bihar, Uttar Pradesh, Maharashtra, West Bengal, and Tamil Nadu for December 2024."

## Key Insight

**Vegetables are running at 20-39% inflation across all five states — this single subgroup is driving the food price crisis.** UP Rural leads at 38.5% vegetable inflation, Bihar at 36.5%.

But the bigger story is in fuel: **rural and urban fuel prices are moving in opposite directions.** In Bihar, rural fuel costs are rising (+0.6%) while urban fuel is falling (-4.7%) — a 5.3 percentage point gap. The same pattern appears in UP (+2.8% rural vs -2.2% urban). Rural households rely on kerosene and firewood; urban households benefit from falling LPG and electricity rates. Food inflation affects both sectors similarly, but fuel inflation is quietly widening the rural-urban cost-of-living divide.

## Findings

### Food & Beverages Inflation — December 2024 (YoY %)

| State | Rural | Urban | Gap |
|-------|-------|-------|-----|
| Bihar | 10.29% | 11.55% | Urban higher by 1.3pp |
| Uttar Pradesh | 9.63% | 9.32% | Rural higher by 0.3pp |
| West Bengal | 8.09% | 6.84% | Rural higher by 1.3pp |
| Tamil Nadu | 6.88% | 6.78% | Nearly equal |
| Maharashtra | 6.52% | 6.76% | Nearly equal |

### Fuel & Light Inflation — December 2024 (YoY %)

| State | Rural | Urban | Gap |
|-------|-------|-------|-----|
| Bihar | **+0.60%** | **-4.74%** | **5.3pp divergence** |
| Uttar Pradesh | **+2.81%** | **-2.24%** | **5.1pp divergence** |
| West Bengal | -3.87% | -5.89% | 2.0pp |
| Tamil Nadu | -0.31% | -2.38% | 2.1pp |
| Maharashtra | -6.46% | -1.91% | 4.6pp (reversed — rural falling faster) |

### What's Driving Food Inflation: Vegetables

| State | Rural | Urban |
|-------|-------|-------|
| Uttar Pradesh | **38.50%** | 34.91% |
| Bihar | 36.51% | 36.32% |
| West Bengal | 32.20% | 21.31% |
| Maharashtra | 20.39% | 19.37% |
| Tamil Nadu | 19.62% | 14.49% |

West Bengal has the widest rural-urban vegetable gap (32% vs 21%) — suggesting supply chain issues in rural distribution.

---

## How It Was Done

CPI data has tens of thousands of rows across 37 states × 3 sectors × 8 groups × 12 months × subgroups. The MCP server's metadata step resolved the arbitrary codes needed to filter precisely.

| Step | Tool | What Happened |
|------|------|---------------|
| 1 | `1_know_about_mospi_api()` | Identified CPI |
| 2 | `2_get_indicators("CPI")` | CPI uses Group/Item levels, not indicator codes |
| 3 | `3_get_metadata("CPI", base_year="2012", level="Group")` | Discovered state codes (UP=9, MH=27, Bihar=10, WB=19, TN=33), group codes (Food=1, Fuel=5), sector codes (Rural=1, Urban=2) |
| 4 | `4_get_data("CPI", ...)` | Fetched 200+ records: 5 states × 2 groups × 2 sectors, with all subgroup breakdowns for Dec 2024 |

**4 API calls.** The state codes differ between CPI and PLFS datasets — the metadata step resolves this automatically each time.
