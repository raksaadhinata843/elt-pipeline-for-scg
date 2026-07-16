# ELT Pipeline for SCG

An automated Extract-Load-Transform (ELT) data pipeline that ingests commodity prices (cement, coal, and oil) from public APIs, stages them through a medallion architecture (Bronze/Silver/Gold), and generates energy-to-cement correlation analysis for supply chain insights.

## Stack

- **Language**: Python 3.12
- **Framework**: AWS Lambda (serverless orchestration)
- **Database**: DuckDB (in-process OLAP analytics)
- **Data Storage**: Amazon S3 (parquet format)
- **CI/CD**: GitHub Actions
- **APIs**: FRED (Federal Reserve Economic Data) for cement & coal, EIA (U.S. Energy Information Administration) for oil

## How it's organized

```
src/
  bronze/           Data ingestion layer
    ingestion_coal.py        AWS Lambda: fetch coal prices from FRED API
    ingestion_cement.py      AWS Lambda: fetch cement prices from FRED API
    ingestion_oil.py         AWS Lambda: fetch oil prices from EIA API
  silver/           Data transformation & cleaning
    ddl/
      ddl_coal.sql           Deduplication & type conversion for coal
      ddl_cement.sql         Deduplication & type conversion for cement
      ddl_oil.sql            Deduplication & type conversion for oil
    profiling_data/          Data quality checks
      check_cement.sql
    final_check_quality.sql  Data profiling summary across all silver tables
  gold/             Analytics & features
    unified.sql              Merge cement, coal, oil by monthly period
    indexed.sql              Normalize all prices to base-100 index (Jan 2010)
    gap.sql                  Calculate energy availability gap vs cement demand
    lag.sql                  Lag features for time-series modeling
.github/workflows/
  main.yml                   GitHub Actions workflow orchestrating bronze → silver → gold
DATA_DOCS.md                 Auto-generated data documentation
docs/
  docs.sql                   DuckDB script to generate DATA_DOCS.md
  index.html                 Embedded dashboard (Preset/Superset)
```

## How it fits together

The pipeline runs as a manual GitHub Actions workflow (triggered via `workflow_dispatch`):

1. **Bronze Layer** (Ingestion): Three parallel Lambda functions fetch commodity prices from public APIs and write raw parquet files to S3 by date partition (year/month/day).

2. **Silver Layer** (Cleaning): DuckDB SQL transforms read raw data, convert types, and deduplicate records (keeping latest version by `realtime_start` timestamp for FRED data).

3. **Gold Layer** (Analytics): Unified table merges cement/coal/oil by month, creates a normalized price index (base 100 = Jan 2010), and generates gap analysis & lag features for modeling cement demand drivers.

4. **Documentation**: Auto-generated `DATA_DOCS.md` profiles the final unified dataset; an embedded Superset dashboard provides visual exploration.

## How to run it

### Prerequisites

- AWS account with Lambda, S3 access and credentials configured
- GitHub secrets configured:
  - `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`
  - `S3_BUCKET` (e.g., `my-scg-data`)
  - `FRED_API_KEY` (from [Federal Reserve Economic Data](https://fred.stlouisfed.org/docs/api/api_key.html))
  - `EIA_API_KEY` (from [EIA API](https://www.eia.gov/opendata/))

### Run the Pipeline

Trigger via GitHub UI or CLI:

```bash
# Run all three datasets (default)
gh workflow run main.yml

# Or run a specific dataset
gh workflow run main.yml -f target=coal
gh workflow run main.yml -f target=cement
gh workflow run main.yml -f target=oil
```

The workflow:
1. Packages each ingestion script as a Lambda deployment
2. Updates Lambda code and configuration
3. Invokes Lambda functions to fetch data
4. Runs silver-layer SQL transformations
5. Runs gold-layer analytics queries

### View Results

After workflow completes:
- Raw data: `s3://my-scg-data/bronze/{energy,construction}/{coal,cement,oil}/year=*/month=*/day=*/*.parquet`
- Cleaned data: DuckDB tables `silver_coal`, `silver_cement`, `silver_oil`
- Analytics: DuckDB tables `gold_unified`, `gold_indexed`, `gold_gap`, `gold_lag`
- Dashboard: Visit the embedded Superset link in `docs/index.html`

## Known Issues & To-Do

### 🐛 Bug: Coal Ingestion Data Quality

**File**: `src/bronze/ingestion_coal.py` (lines 47–54)

**Issue**: Invalid observations (with value ".") are filtered from `raw["observations"]` *after* the pandas DataFrame is already created from them. This means the DataFrame still contains null/invalid values that were supposed to be excluded.

**Current Code**:
```python
df = pd.DataFrame(raw["observations"])
df["value"] = pd.to_numeric(df["value"], errors='coerce')  # Converts "." to NaN
df["date"] = pd.to_datetime(df["date"])

# Filter happens AFTER DataFrame creation — too late!
raw["observations"] = [
    obs for obs in raw["observations"] 
    if obs["value"] != "."
]
```

**Fix**: Filter the raw observations **before** creating the DataFrame:
```python
# Filter first
raw["observations"] = [
    obs for obs in raw["observations"] 
    if obs["value"] != "."
]

# Then create DataFrame from cleaned data
df = pd.DataFrame(raw["observations"])
df["value"] = pd.to_numeric(df["value"], errors='coerce')
df["date"] = pd.to_datetime(df["date"])
```

This ensures only valid price records are persisted to S3.

## Try asking

- "How do I add a new commodity (e.g., natural gas) to the pipeline?"
- "What's the data lineage from raw API response to the gap_total_energy metric?"
- "How often does the pipeline need to run to stay current with FRED and EIA data?"
- "Can I query historical gap analysis for specific time periods?"
