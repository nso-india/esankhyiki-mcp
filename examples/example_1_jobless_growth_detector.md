# Example 1: The Jobless Growth Detector

## Query

> "Is India's GDP growth actually translating into jobs? Compare the GDP growth rate with the unemployment rate for youth (15-29 years) vs the overall population (15+), broken down by rural and urban, from 2017-18 to 2023-24."

## Key Insight

**India's "jobless growth" narrative is dead — but a youth crisis persists.** Overall unemployment nearly halved from 6.0% to 3.2% alongside sustained GDP growth (averaging 5.4% including the COVID shock). However, youth unemployment (15-29) remains stubbornly at 3x the overall rate (10.2% in 2023-24), and the rural-urban divide is widening: rural unemployment fell to 2.5% while urban sits at 5.1%.

The most striking pattern: during COVID (2020-21, GDP: -5.8%), unemployment actually *fell* to 4.2% — likely because workers shifted into agriculture and self-employment rather than remaining formally "unemployed." The data reveals disguised unemployment, not genuine recovery.

## Findings

### GDP Growth vs Unemployment (Combined, All India)

| Year | GDP Growth (%) | UR Overall (15+) | UR Youth (15-29) | Youth/Overall Ratio |
|------|---------------|-------------------|-------------------|---------------------|
| 2017-18 | 6.80 | 6.0% | 17.8% | 3.0x |
| 2018-19 | 6.45 | 5.0% (R) / 7.6% (U)* | 16.0% (R) / 20.2% (U)* | ~3.0x |
| 2019-20 | 3.87 | 4.8% | 15.0% | 3.1x |
| 2020-21 | -5.78 | 4.2% | 12.9% | 3.1x |
| 2021-22 | 9.69 | 4.1% | 12.4% | 3.0x |
| 2022-23 | 7.61 | 3.2% | 10.0% | 3.1x |
| 2023-24 | 9.19 | 3.2% | 10.2% | 3.2x |

*\*2018-19 combined value fell outside retrieved API page; sector-level values shown.*

### Rural vs Urban Breakdown

| Year | Rural (15+) | Urban (15+) | Rural Youth | Urban Youth |
|------|------------|------------|-------------|-------------|
| 2017-18 | 5.3% | 7.7% | 16.6% | 20.6% |
| 2019-20 | 3.9% | 6.9% | 12.9% | 19.9% |
| 2020-21 | 3.3% | 6.7% | 10.7% | 18.5% |
| 2022-23 | 2.4% | 5.4% | 8.0% | 15.7% |
| 2023-24 | 2.5% | 5.1% | 8.5% | 14.7% |

Urban youth (14.7%) remain the most vulnerable cohort even in a 9.2% GDP growth year.

---

## How It Was Done

This required combining **two different datasets** — NAS for GDP and PLFS for unemployment — each with their own structures and filter codes.

| Step | Tool | What Happened |
|------|------|---------------|
| 1 | `1_know_about_mospi_api()` | Identified NAS for GDP, PLFS for unemployment |
| 2 | `2_get_indicators("NAS")` | Found indicator 22 = GDP Growth Rate |
| 2 | `2_get_indicators("PLFS")` | Found indicator 3 = Unemployment Rate |
| 3 | `3_get_metadata("NAS", ...)` | Got revision codes, year formats, approach codes |
| 3 | `3_get_metadata("PLFS", ...)` | Got age codes (1=15+, 2=15-29), sector codes (1=rural, 2=urban), state code 99=All India |
| 4 | `4_get_data("NAS", ...)` | Fetched GDP growth 2017-24, all revisions; AI selected latest revision per year |
| 4 | `4_get_data("PLFS", ...)` | Fetched UR by age × sector × year, 30 clean data points |

**6 API calls across 2 datasets.** The GDP data returned multiple revisions per year (First Advance through Final Estimates) — the AI selected the most recent revision automatically.
