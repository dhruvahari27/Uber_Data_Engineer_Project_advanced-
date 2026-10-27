# Lakehouse Pipeline Code & Notebooks

This folder contains the core Delta Live Tables (DLT) streaming pipeline code and exploration notebooks for Azure Databricks.

## Architecture Flow

```
[Azure Event Hubs / Kafka] ───► ingest.py (rides_raw)
                                    │
[ADLS Gen2 / JSON Mappings] ───────┼───► silver.py (stg_rides)
                                    │          │
                                    │          ▼
                                    └───► silver_obt.sql (silver_obt)
                                               │
                                               ▼
                                          model.py (Star Schema: Dims + Fact)
```

## File Manifest

| File | Type | Description |
|---|---|---|
| `ingest.py` | DLT Pipeline (Bronze) | Connects to Azure Event Hubs via Kafka SASL_SSL to ingest raw ride JSON events into `rides_raw`. |
| `silver.py` | DLT Pipeline (Silver) | Declares `stg_rides` streaming table with two append flows: historical batch (`bulk_rides`) and parsed stream (`rides_stream`). |
| `silver_obt.sql` | DLT Pipeline (Silver OBT) | Joins streaming staged rides with reference mapping tables into a denormalized One Big Table (`silver_obt`) with 3-minute watermarking. |
| `model.py` | DLT Pipeline (Gold) | Builds the Dimensional Star Schema using Delta Live Tables Auto-CDC: SCD Type 1 (`dim_passenger`, `dim_driver`, `dim_vehicle`, `dim_payment`, `dim_booking`, `fact`) and SCD Type 2 (`dim_location`). |
| `bronze_adls.ipynb` | Databricks Notebook | Ingestion helper script for loading mapping JSONs and bulk rides from ADLS Gen2 into Delta tables. |
| `silver_obt.ipynb` | Databricks Notebook | Development notebook for testing schemas, dynamic Jinja2 SQL generation, and table validations. |
