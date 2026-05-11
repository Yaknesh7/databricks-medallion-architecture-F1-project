# Formula 1 Data Lakehouse Project

## Overview

This project demonstrates the implementation of a modern end-to-end Data Lakehouse solution using Azure Databricks and PySpark. The solution follows Medallion Architecture with Landing, Bronze, Silver, and Gold layers for scalable, reliable, and analytics-ready data processing.

The project includes:
- Full Refresh Processing Framework
- Incremental Batch Processing Framework
- Delta Lake implementation
- Unity Catalog governance
- Databricks Lakeflow Jobs orchestration
- Analytics-ready Gold layer datasets

The project processes Formula 1 datasets and transforms raw data into business-ready analytical datasets for reporting and insights.

---

# Solution Architecture

## Full Refresh Architecture

![Full Refresh Architecture](architecture/full-refresh-architecture.png)

---

## Incremental Processing Architecture

![Incremental Processing Architecture](architecture/incremental-processing-architecture.png)

---

# Medallion Architecture

The project follows the Medallion Architecture design pattern.

| Layer | Description |
|---|---|
| Landing | Raw source files stored without transformations |
| Bronze | Schema-enforced raw ingestion layer with metadata tracking |
| Silver | Cleaned, standardized, and transformed datasets |
| Gold | Business-ready analytical datasets optimized for reporting |

---

# Technologies Used

| Category | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Data Platform | Azure Databricks |
| Processing Engine | PySpark |
| Query Language | Spark SQL |
| Storage | Azure Data Lake Storage Gen2 (ADLS Gen2) |
| Data Format | Delta Lake |
| Governance | Unity Catalog |
| Workflow Orchestration | Databricks Lakeflow Jobs |
| Programming Language | Python |

---

# Data Lake & Unity Catalog Configuration

The project uses Azure Data Lake Storage Gen2 (ADLS Gen2) as the centralized storage layer for the Data Lakehouse architecture.

## Storage Configuration Steps

1. Created Azure Data Lake Storage Gen2 account for storing Landing, Bronze, Silver, and Gold layer data.
2. Configured Databricks Access Connector to securely connect Azure Databricks with ADLS Gen2.
3. Assigned required IAM permissions including Storage Blob Data Contributor role.
4. Created Storage Credential in Unity Catalog using the configured Access Connector.
5. Configured External Location in Unity Catalog mapped to ADLS Gen2 containers.
6. Utilized Unity Catalog for centralized governance, schema management, and secure external data access.

---

## Architecture Components

| Component | Purpose |
|---|---|
| ADLS Gen2 | Centralized cloud storage |
| Databricks Access Connector | Secure authentication between Databricks and ADLS |
| Storage Credential | Unity Catalog authentication object |
| External Location | Secure external storage mapping |
| Unity Catalog | Governance and data organization |

---

# Project Workflow

Landing → Bronze → Silver → Gold

## Landing Layer
- Raw source files stored in ADLS Gen2
- Controlled ingestion point
- Source data preservation

## Bronze Layer
- Schema enforcement
- Metadata columns added
- Delta Lake ingestion
- Audit tracking
- Raw historical data preservation

## Silver Layer
- Data cleansing
- Standardization
- Deduplication
- Flattening nested JSON structures
- Trusted and transformed datasets

## Gold Layer
- Fact tables
- Dimension tables
- Aggregated analytical datasets
- BI-ready reporting layer

---

# Datasets Used

The project processes the following Formula 1 datasets:

- Circuits
- Races
- Constructors
- Drivers
- Results
- Sprints

---

# Key Features

- End-to-end ETL pipeline implementation
- Medallion Architecture
- Full Refresh Processing
- Incremental Batch Processing
- Delta Lake implementation
- Schema enforcement
- Metadata tracking
- Data cleansing and transformation
- Dimensional data modeling
- Workflow orchestration using Lakeflow Jobs
- Unity Catalog governance
- Analytics-ready Gold layer datasets

---

# Full Refresh Processing Framework

The Full Refresh pipeline performs complete data reloads from source datasets into the Data Lakehouse layers.

## Full Refresh Workflow

1. Raw source files loaded into Landing layer
2. Bronze layer ingestion executed
3. Silver layer transformations applied
4. Gold layer analytical datasets generated
5. Reporting-ready tables updated

---

# Incremental Processing Framework

The project implements a production-style incremental data processing framework using:

- Batch ID-based ingestion
- Batch control table
- Incremental merge/upsert logic
- Process status tracking
- Automated orchestration workflows

---

## Incremental Processing Workflow

1. New batch data arrives
2. Batch ID generated
3. Data ingested into Bronze layer
4. Silver transformations applied
5. Gold layer updated
6. Batch status updated in control table

---

## Incremental Processing Benefits

- Reduced processing time
- Faster execution
- Efficient resource utilization
- Scalable ETL architecture
- Production-style pipeline implementation

---

# Unity Catalog Implementation

The project utilizes Unity Catalog for:

- Centralized governance
- Schema management
- Table organization
- Secure external data access
- Data lineage and management

---

# Delta Lake Implementation

Delta Lake is used across Bronze, Silver, and Gold layers to provide:

- ACID transactions
- Schema enforcement
- Time travel support
- Reliable incremental processing
- Improved performance and scalability

---

# Workflow Orchestration

Databricks Lakeflow Jobs are used for:

- Automated pipeline execution
- Task dependency management
- Retry mechanisms
- Monitoring and alerting
- Scheduled execution

---

# Screenshots

## Unity Catalog

![Unity Catalog](screenshots/unity-catalog.png)

---

## Databricks Lakeflow Jobs

![Databricks Jobs](screenshots/databricks-jobs.png)

---

## Bronze Layer

![Bronze Layer](screenshots/bronze-layer.png)

---

## Silver Layer

![Silver Layer](screenshots/silver-layer.png)

---

## Gold Layer

![Gold Layer](screenshots/gold-layer.png)

---

## Incremental Processing

![Incremental Processing](screenshots/incremental-processing.png)

---

## PySpark Notebook

![PySpark Notebook](screenshots/pyspark-notebook.png)

---

## Dashboard Output

![Dashboard Output](screenshots/dashboard-output.png)

---

# Repository Structure

```text
databricks-medallion-architecture-F1-project/
│
├── architecture/
│   ├── full-refresh-architecture.png
│   ├── incremental-processing-architecture.png
│
├── notebooks/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── incremental/
│   └── common/
│
├── screenshots/
│
├── docs/
│
├── datasets/
│
├── README.md
└── requirements.txt
```

---

# Key Learnings

- Distributed data processing using Apache Spark
- Data Lakehouse implementation
- Medallion Architecture design
- Incremental ETL pipeline development
- Delta Lake optimization
- Workflow orchestration in Databricks
- Data governance using Unity Catalog
- Cloud storage integration with ADLS Gen2
- Production-style Data Engineering workflows

