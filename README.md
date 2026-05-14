# NYC Taxi — Azure Databricks Personal Project

A personal end-to-end data engineering project built on **Azure Databricks** and **Apache Spark**. The pipeline ingests NYC Yellow Taxi trip data, processes it through a Medallion Architecture (Landing → Bronze → Silver → Gold), and exports the final result to Azure Blob Storage (ADLS Gen2).

***

## 🗂️ Project Structure

```
databricks_personal_project/
├── transformations/
│   └── notebooks/
│       ├── 00_landing/         # Data ingestion from external source
│       ├── 01_bronze/          # Raw data load into Delta table
│       ├── 02_silver/          # Data cleansing & enrichment
│       ├── 03_gold/            # Aggregations & business-ready tables
│       └── 04_export/          # Export to Blob Storage
├── modules/
│   ├── data_loader/            # File download utilities
│   ├── transformations/        # Reusable transformation functions
│   └── utils/                  # Date helpers and shared utilities
├── ad_hoc/                     # One-time exploratory notebooks
└── one_off/                    # One-off scripts
```

***

## ⚙️ Pipeline Overview

### 00 — Landing
Downloads NYC Yellow Taxi `.parquet` files from the [TLC public dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) for the month that is **2 months prior** to the current date. Uses `dbutils.jobs.taskValues` to signal downstream tasks whether to continue or skip (file already exists check).

### 01 — Bronze
Reads the raw `.parquet` file from the Databricks Volume (`/Volumes/nyctax/00_landing/...`) and appends it to the Delta table `nyctax.01_bronze.yellow_trips_row`, adding a `processed_timestamp` metadata column.

### 02 — Silver
- **`yellow_trips_cleansed.py`** — removes nulls, fixes data types, filters invalid records
- **`yellow_trips_enriched.py`** — joins with taxi zone lookup, adds derived columns (e.g. `year_month`)
- **`taxi_zone_lookup.py`** — processes the reference lookup table for pickup/dropoff zones

### 03 — Gold
**`daily_trip_summary.py`** — aggregates cleansed trip data into a daily summary table (`nyctax.03_gold.daily_trip_summary`) with metrics like trip counts and total fares.

### 04 — Export
Exports the enriched Silver table to **Azure Blob Storage (ADLS Gen2)** partitioned by `vendor` and `year_month`, and registers it as an external table `nyctax.04_export.yellow_trips_export`.

***

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| Azure Databricks | Compute & orchestration |
| Apache Spark (PySpark) | Data processing |
| Delta Lake | Table format (ACID, versioning) |
| Azure Blob Storage (ADLS Gen2) | Data Lake storage |
| Unity Catalog | Data governance & schema management |
| Python | Scripting & reusable modules |

***

## 📐 Architecture

```
[NYC TLC Public API]
        ↓
  00_landing (Parquet → Volume)
        ↓
  01_bronze (Raw Delta table)
        ↓
  02_silver (Cleansed + Enriched Delta tables)
        ↓
  03_gold (Aggregated Delta table)
        ↓
  04_export (JSON → Azure Blob Storage / ADLS Gen2)
```

***

## 📦 Modules

- **`modules/data_loader/file_downloader.py`** — downloads files from URL to Databricks Volumes
- **`modules/transformations/metadata.py`** — adds `processed_timestamp` to DataFrames
- **`modules/utils/date_utils.py`** — returns formatted target date (e.g. `get_target_yyyymm(months_ago=2)`)

***

## 🚀 How to Run

1. Clone the repo and upload notebooks to your Databricks workspace
2. Create a Unity Catalog schema: `nyctax` with layers `00_landing`, `01_bronze`, `02_silver`, `03_gold`, `04_export`
3. Set up an Azure Blob Storage account and container, mount it or configure ADLS Gen2 access
4. Run notebooks in order: `00 → 01 → 02 → 03 → 04`
5. (Optional) Set up a Databricks Job with task dependencies using `taskValues` for conditional execution

***

## 🎯 Project Goals

- Learn how **Databricks** and **Apache Spark** work together in a real pipeline
- Practice **Medallion Architecture** (Landing / Bronze / Silver / Gold)
- Understand **Delta Lake** as a reliable table format
- Work with **Azure cloud storage** (Blob Storage / ADLS Gen2)
- Build reusable Python **modules** for a data engineering project
