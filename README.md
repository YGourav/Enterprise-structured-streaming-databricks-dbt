# Enterprise-Grade Structured Streaming Data Platform  
### PySpark Structured Streaming | Databricks | Delta Lake | DBT

## 📌 Project Overview
This project implements an **enterprise-grade, end-to-end data engineering pipeline** for an Uber-like domain using **Databricks**, **PySpark Structured Streaming**, **Delta Lake**, and **DBT Cloud**.

The pipeline is designed to handle **incremental data ingestion**, **Change Data Capture (CDC)**, **upserts**, and **Slowly Changing Dimensions (SCD Type 2)** using **Medallion Architecture (Bronze, Silver, Gold)**.

---

## 🏗 Architecture Overview

**Source → Bronze → Silver → Gold**

- **Source**: Incrementally arriving CSV files (customers, drivers, trips, vehicles, locations, payments)
- **Bronze**: Raw ingestion using Structured Streaming
- **Silver**: Cleaned and deduplicated data with CDC handling
- **Gold**: Analytics-ready fact and dimension tables using DBT

---

## 🛠 Tech Stack

- **Databricks Free Edition**
- **PySpark Structured Streaming**
- **Delta Lake**
- **DBT Cloud**
- **Jinja SQL**
- **Python (Modular Classes)**

---

## 📥 Bronze Layer – Streaming Ingestion

- Incremental file ingestion using **Spark Structured Streaming**
- Schema inferred once and reused for streaming
- Checkpointing enabled for:
  - Exactly-once processing
  - Fault tolerance
- Data written to Delta tables in append mode

**Why Structured Streaming?**
- Files arrive continuously
- Avoids full reprocessing
- Scales efficiently

---

## 🧹 Silver Layer – Transformations & Data Quality

Implemented using **modular Python classes** to ensure reusability and maintainability.

### Key Features:
- Deduplication using window functions
- CDC-based upserts using Delta Lake MERGE
- Processing timestamp for auditing
- Entity-specific transformations (email parsing, phone cleanup, standardization)

### Upsert Logic:
- Records are updated only if incoming CDC timestamp is newer
- New records are inserted automatically
- Ensures idempotency and correctness

---

## 📊 Gold Layer – Analytics Engineering with DBT

- Bronze and Silver tables registered as DBT sources
- Incremental fact table models using DBT
- **SCD Type 2 implemented via DBT snapshots** for all dimension tables
- Dynamic SQL using Jinja templating
- Schema customization via DBT macros

---

## 🧠 Key Data Engineering Concepts Implemented

- Structured Streaming
- Medallion Architecture
- Incremental processing
- Delta Lake MERGE (Upserts)
- Change Data Capture (CDC)
- Slowly Changing Dimensions (SCD Type 2)
- Modular & reusable code
- Analytics engineering with DBT

---

## 🎯 Final Outcome

- Scalable, fault-tolerant streaming data pipeline
- Analytics-ready fact and dimension tables
- Enterprise-grade design aligned with real-world data platforms

---

## 🚀 How This Project Stands Out

- Streaming-first ingestion =
- Correct CDC handling with Delta Lake
- Proper SCD Type 2 implementation
- Combines PySpark + SQL + DBT
- Matches modern enterprise data architectures

---

## 👤 Author
**Gourav Yadav**  
Data Engineer | PySpark | Databricks 
