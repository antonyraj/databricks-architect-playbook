# Lakeflow CDC & SCD Demo

This repository demonstrates a Spark Lakeflow Declarative Pipeline using:
- Streaming ingestion (Auto Loader)
- Data quality expectations
- CDC (SCD Type 1)
- Slowly Changing Dimension Type 2
- Gold analytical table

## Architecture
Bronze → Silver → CDC → SCD2 → Gold

## How to Deploy

```bash
databricks bundle validate
databricks bundle deploy


Then start the pipeline from the Databricks UI.

Key Concepts

Declarative pipelines (no write logic)

Automatic DAG inference

Built-in data quality & lineage

Managed state and recovery


---

# ▶️ How to Run (Step-by-Step)

```bash
git clone https://github.com/you/lakeflow-cdc-scd-demo.git
cd lakeflow-cdc-scd-demo

databricks auth login
databricks bundle deploy


Then:

Go to Pipelines

Start customer-lakeflow-cdc-scd



Azure SQL Database
      ↓ (JDBC / CDC)
Lakeflow Bronze (raw)
      ↓
Silver (clean + validated)
      ↓
CDC / SCD Type 1 & 2
      ↓
Gold (analytics)

## Data Source Variants

- `customer_pipeline_sql.py`  
  Reads customer data from Azure SQL Database using JDBC and applies CDC + SCD logic.

- `customer_pipeline_files.py`  
  File-based ingestion using Auto Loader (demo / local testing).


🧠 Mental Model (Very Important)
Concern	      Where it lives
Source-specific   logic	src/pipelines/*.py
Pipeline          definition	resources/*.yml
Environment       config	databricks.yml
Secrets	      Databricks Secret Scope

File	                        Purpose
customer_pipeline_files.py	Demo / local / quick testing
customer_pipeline_sql.py	Real Azure SQL source

lakeflow-cdc-scd-demo/
│
├── databricks.yml
│
├── resources/
│   └── lakeflow_pipeline.yml   👈 switch source here
│
├── src/
│   └── pipelines/
│       ├── customer_pipeline_files.py   👈 file-based (hard-coded/demo)
│       └── customer_pipeline_sql.py     👈 Azure SQL-based (real)
│
├── data/
│   └── sample/
│       └── customers.json
│
└── README.md

