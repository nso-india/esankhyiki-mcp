# Example 2: India's Education-Unemployment Paradox

## Query

> "Is education helping Indians get jobs? Show me unemployment rates by education level — from illiterate to postgraduate — for All India."

## Key Insight

**The more educated you are in India, the more likely you are to be unemployed.** A postgraduate is 12x more likely to be unemployed than an illiterate person. Diploma holders face the worst outcomes at 19.8% — 16.5x the rate for those with no education (1.2%).

This isn't a data quirk. Illiterate workers take whatever is available — farm labor, construction, daily wages. They can't afford to be unemployed. Graduates and postgraduates queue for "appropriate" jobs (government posts, white-collar roles), creating voluntary wait unemployment. The data suggests India's education system is producing credentials faster than the economy is producing matching jobs.

A second finding: in 2023-24, the gender gap at the overall level has *closed* (both 3.2%), but among youth, women still face 11.0% vs men's 9.8%.

## Findings

### Unemployment Rate by Education (2017-18, All India, Rural+Urban, PS+SS)

| Education Level | UR (%) | vs Illiterate |
|----------------|--------|---------------|
| Not literate | 1.2 | baseline |
| Literate & upto primary | 2.7 | 2.3x |
| Middle | 5.5 | 4.6x |
| Secondary | 5.7 | 4.8x |
| Higher secondary | 10.3 | 8.6x |
| **Diploma/Certificate** | **19.8** | **16.5x** |
| **Graduate** | **17.2** | **14.3x** |
| Post graduate & above | 14.6 | 12.2x |

### Gender × Age (2023-24, All India, Combined)

| Group | Male | Female | Person |
|-------|------|--------|--------|
| Overall (15+) | 3.2% | 3.2% | 3.2% |
| Youth (15-29) | 9.8% | 11.0% | 10.2% |

---

## How It Was Done

Single dataset (PLFS), but the power is in the multi-dimensional slicing — education × sector × gender × age in one query.

| Step | Tool | What Happened |
|------|------|---------------|
| 1 | `1_know_about_mospi_api()` | Identified PLFS |
| 2 | `2_get_indicators("PLFS")` | Found indicator 3 = Unemployment Rate |
| 3 | `3_get_metadata("PLFS", indicator_code=3, frequency_code=1)` | Discovered 10 education codes (e.g. code 6 = diploma, code 8 = postgrad), gender codes, age codes |
| 4 | `4_get_data(...)` | Fetched UR for 2017-18 across all education levels, All India |
| 4 | `4_get_data(...)` | Fetched UR by gender × age for 2023-24 |

**5 API calls.** The key enabler is metadata discovery — you'd need to know that `education_code=6` means "diploma/certificate" to filter correctly. The MCP server surfaces these mappings automatically.
