# End-to-End Databricks ETL Pipeline using Delta Live Tables

## 📌 Overview
This project implements a production-grade end-to-end ETL pipeline using
Azure SQL as the source and Databricks Delta Live Tables (DLT) following
the Medallion Architecture (Bronze, Silver, Gold).

The pipeline supports streaming + batch processing, data quality enforcement,
slowly changing dimensions, and BI-ready analytics.

---

## 🧱 Architecture
**Source → Bronze → Silver → Gold → BI**

- Source: Azure SQL (AdventureWorksLT)
- Processing: Databricks + Delta Live Tables
- Governance: Unity Catalog
- Analytics: Databricks SQL & Power BI
- Version Control: GitHub via Databricks Repos

---

## 🥉 Bronze Layer
- Ingest data from Azure SQL using JDBC
- Incremental loading using ModifiedDate
- Stored as Delta tables in Unity Catalog

---

## 🥈 Silver Layer
- Generic data cleaning
- Column normalization
- Stream + batch hybrid joins
- Customer SCD Type-2 using DLT
- Data quality enforced using DLT expectations

---

## 🥇 Gold Layer
- Business KPIs
- Daily revenue
- Revenue by product category
- Customer lifetime sales metrics
- Revenue by region

---

## 📊 BI & Reporting
- Databricks SQL Dashboards
- Power BI (DirectQuery)
- Materialized views for low-latency analytics

---

## 🚀 Key Features
- Delta Live Tables (DLT)
- Deterministic surrogate keys
- Data quality validation
- Medallion architecture
- BI-ready materialized views

---

## ▶️ How to Run
1. Configure Azure SQL credentials using Databricks Secrets
2. Create DLT pipelines using the provided Python files
3. Run pipelines from Databricks UI
4. Query Gold tables or materialized views
5. Build dashboards in Databricks SQL or Power BI

---

## 👤 Author
**Prashant Garg**  
Data Engineering | Databricks | Azure
