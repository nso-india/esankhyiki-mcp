# Example 5: India's Energy Transition — Reality Check

## Query

> "How has India's energy supply mix changed from 2012-13 to 2023-24? Show the supply of coal, oil, natural gas, and renewables in KToE. Calculate each source's share. Is India actually transitioning away from fossil fuels?"

## Key Insight

**Coal's share of India's primary energy supply *increased* from 56% to 59% over 12 years.** India added 201,438 KToE of coal — dwarfing the 15,198 KToE added by solar, wind, and other renewables. Renewables grew 4x in absolute terms (impressive), but their share moved from just 0.9% to 2.2% of total supply (not enough).

The real transition happening is **oil → coal**, not fossil → renewable. Crude oil's share fell from 38.3% to 29.8% (-8.5pp), and that gap was filled primarily by coal (+3.1pp), not clean energy. India's energy appetite grew 52% in 12 years — so fast that even rapid renewable additions get diluted before they can shift the mix.

Nuclear and hydro are flat at ~1.4% each. Despite policy announcements, neither has moved the needle in a decade.

## Findings

### Total Primary Energy Supply by Source (KToE)

| Year | Coal | Crude Oil | Natural Gas | Solar/Wind/Other | Nuclear | Hydro | Total |
|------|------|-----------|-------------|------------------|---------|-------|-------|
| 2012-13 | 332,660 | 227,553 | 53,623 | 5,091 | 8,565 | 9,790 | 593,601 |
| 2016-17 | 383,119 | 255,439 | 52,198 | 7,209 | 9,881 | 10,537 | 684,456 |
| 2020-21 | 399,768 | 231,947 | 57,076 | 13,279 | 11,214 | 12,955 | 710,062 |
| 2023-24 | 534,098 | 269,417 | 63,115 | 20,289 | 12,493 | 11,559 | 903,158 |

### Computed Shares (%)

| Source | 2012-13 | 2023-24 | Change |
|--------|---------|---------|--------|
| Coal | 56.0% | **59.1%** | +3.1pp |
| Crude Oil | 38.3% | 29.8% | -8.5pp |
| Natural Gas | 9.0% | 7.0% | -2.0pp |
| Solar/Wind/Other | 0.9% | **2.2%** | +1.4pp |
| Nuclear | 1.4% | 1.4% | 0.0pp |
| Hydro | 1.6% | 1.3% | -0.4pp |

### Absolute Growth (2012-13 → 2023-24)

| Source | Added (KToE) | Growth |
|--------|-------------|--------|
| Coal | +201,438 | +61% |
| Solar/Wind/Other | +15,198 | +299% |
| Crude Oil | +41,864 | +18% |
| Total Supply | +309,557 | +52% |

Even during COVID (2020-21), when total supply fell 7%, coal's share *rose* to 56.3%.

---

## How It Was Done

| Step | Tool | What Happened |
|------|------|---------------|
| 1 | `1_know_about_mospi_api()` | Identified ENERGY dataset |
| 2 | `2_get_indicators("ENERGY")` | Found indicator 1 = Energy Balance (KToE), use_of_energy_balance_code 1 = Supply |
| 3 | `3_get_metadata("ENERGY", indicator_code=1, use_of_energy_balance_code=1)` | Discovered 10 commodity codes (Coal=1, Crude Oil=2, Solar/Wind=7, etc.) and end_use_sector_code 9 = Total primary energy supply |
| 4 | `4_get_data("ENERGY", ...)` | Fetched 108 records: 9 commodities × 12 years |

**4 API calls, 108 data points.** The percentage shares were computed from the raw KToE values — they don't exist as fields in the dataset. The commodity codes (e.g. Solar/Wind = code 7) are arbitrary and only discoverable via the metadata step.
