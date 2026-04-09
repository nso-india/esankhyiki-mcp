# Example 3: Capital vs. Labor in Indian Manufacturing

## Query

> "Which manufacturing industries are becoming more capital-intensive? For pharmaceuticals, textiles, and motor vehicles, compute fixed capital per worker and GVA per worker from 2008-09 to 2023-24. Is automation replacing labor?"

## Key Insight

**Automation is not replacing labor in Indian manufacturing — all three industries added workers massively.** Motor vehicles grew from 393K to 1.1M workers (+182%), pharma from 238K to 625K (+162%), and even textiles added 22%. India's manufacturing story is expansion with capital deepening, not labor substitution.

The surprise: motor vehicles is *hiring faster than it's adding capital*. Its fixed capital per worker actually fell from ₹26.33L to ₹22.66L between 2020-21 and 2023-24, while its GVA per worker surged to ₹20.19L (+185% over 15 years). This sector is scaling both dimensions simultaneously.

Textiles tells a different story: 1.45M workers (the most of the three) but only ₹6.59L of GVA per worker — 4.3x less productive than pharma. It's India's biggest manufacturing employer and its least productive.

## Findings

### Pharma (NIC 21)

| Year | Fixed Capital (₹ Cr) | Workers | GVA (₹ Cr) | FC/Worker (₹ L) | GVA/Worker (₹ L) |
|------|---------------------|---------|-------------|-----------------|-------------------|
| 2008-09 | 42,334 | 237,966 | 35,076 | 17.79 | 14.74 |
| 2016-17 | 109,843 | 431,732 | 97,545 | 25.44 | 22.60 |
| 2023-24 | 189,001 | 624,563 | 177,917 | **30.26** | **28.49** |

FC/Worker +70% | GVA/Worker +93% | Workers **+162%**

### Motor Vehicles (NIC 29)

| Year | Fixed Capital (₹ Cr) | Workers | GVA (₹ Cr) | FC/Worker (₹ L) | GVA/Worker (₹ L) |
|------|---------------------|---------|-------------|-----------------|-------------------|
| 2008-09 | 67,966 | 392,964 | 27,841 | 17.30 | 7.09 |
| 2016-17 | 185,066 | 766,815 | 105,046 | 24.13 | 13.70 |
| 2023-24 | 251,333 | 1,109,398 | 223,933 | **22.66** | **20.19** |

FC/Worker +31% | GVA/Worker +185% | Workers **+182%**

### Textiles (NIC 13)

| Year | Fixed Capital (₹ Cr) | Workers | GVA (₹ Cr) | FC/Worker (₹ L) | GVA/Worker (₹ L) |
|------|---------------------|---------|-------------|-----------------|-------------------|
| 2008-09 | 86,095 | 1,193,635 | 27,707 | 7.21 | 2.32 |
| 2016-17 | 155,744 | 1,332,482 | 66,347 | 11.69 | 4.98 |
| 2023-24 | 200,690 | 1,452,833 | 95,703 | **13.81** | **6.59** |

FC/Worker +92% | GVA/Worker +184% | Workers **+22%**

---

## How It Was Done

This required fetching **three separate indicators** (Fixed Capital, GVA, Total Workers) for **three NIC industry codes** across 16 years, then computing derived ratios that don't exist in the raw data.

| Step | Tool | What Happened |
|------|------|---------------|
| 1 | `1_know_about_mospi_api()` | Identified ASI for factory-level data |
| 2 | `2_get_indicators("ASI")` | Found 57 indicators; selected #3 (Fixed Capital), #19 (GVA), #32 (Total Workers) |
| 3 | `3_get_metadata("ASI", classification_year="2008")` | Got NIC codes: 13=Textiles, 21=Pharma, 29=Motor Vehicles; confirmed years 2008-09 to 2023-24 |
| 4 | `4_get_data(...)` page 1 | Fetched 100 records — 3 indicators × 3 industries × recent years |
| 4 | `4_get_data(...)` page 2 | Fetched remaining 44 records for earlier years |

**5 API calls, 144 data points.** The FC/Worker and GVA/Worker ratios were computed from the raw indicator values — these derived metrics don't exist in the dataset itself.
