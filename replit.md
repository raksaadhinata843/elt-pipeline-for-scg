# ELT Pipeline for SCG

An automated Extract-Load-Transform (ELT) data pipeline that ingests commodity prices (cement, coal, and oil) from public APIs, stages them through a medallion architecture (Bronze/Silver/Gold), and generates energy-to-cement correlation analysis for supply chain insights.

## Stack

- **Language**: Python 3.12
- **Orchestration**: AWS Lambda + GitHub Actions (`workflow_dispatch`)
- **Analytics**: DuckDB (in-process OLAP)
- **Storage**: Amazon S3 (Parquet format)
- **APIs**: FRED (Federal Reserve Economic Data) for cement & coal; EIA (U.S. Energy Information Administration) for oil

## Project structure

```
src/
  bronze/           Ingestion layer — three AWS Lambda functions
    ingestion_coal.py
    ingestion_cement.py
    ingestion_oil.py
  silver/           Cleaning & deduplication (DuckDB SQL)
    ddl/
      ddl_coal.sql
      ddl_cement.sql
      ddl_oil.sql
    profiling_data/          Data quality checks
    final_check_quality.sql
  gold/             Analytics (DuckDB SQL)
    unified.sql    Merge all three commodities by month
    indexed.sql    Normalize to base-100 index (Jan 2010)
    gap.sql        Energy availability gap vs cement demand
    lag.sql        Lag features for time-series modeling
.github/workflows/
  main.yml         GitHub Actions orchestration (bronze → silver → gold)
docs/
  docs.sql         Generates DATA_DOCS.md
  index.html       Embedded Superset dashboard
DATA_DOCS.md       Auto-generated data documentation
```

## How to run

The pipeline runs via GitHub Actions (`workflow_dispatch`) — it is **not** executed locally on Replit.

### Prerequisites (GitHub Secrets)
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`
- `S3_BUCKET` — target S3 bucket name
- `FRED_API_KEY` — from https://fred.stlouisfed.org/docs/api/api_key.html
- `EIA_API_KEY` — from https://www.eia.gov/opendata/

### Trigger the pipeline
```bash
gh workflow run main.yml              # run all three datasets (default)
gh workflow run main.yml -f target=coal
gh workflow run main.yml -f target=cement
gh workflow run main.yml -f target=oil
```

## Known issues

**Coal ingestion bug** (`src/bronze/ingestion_coal.py`, lines 47–54): invalid observations (value `"."`) are filtered *after* the DataFrame is created, so NaN values still reach S3. The filter should run *before* `pd.DataFrame(raw["observations"])` is called. Details and the fix are in the README.

## User preferences

- Imported for code exploration — no run workflow needed on Replit.
