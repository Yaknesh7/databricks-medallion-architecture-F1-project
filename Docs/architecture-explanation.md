# Architecture Explanation

## Overview

This project implements a modern end-to-end Data Lakehouse architecture using Azure Databricks and PySpark. The solution is designed using Medallion Architecture to build scalable, governed, and analytics-ready data pipelines.

The architecture supports both:
- Full Refresh Processing
- Incremental Batch Processing

The complete pipeline is built on Azure Data Lake Storage Gen2 (ADLS Gen2) integrated with Unity Catalog for centralized governance and secure data access.

The solution consists of the following layers:
- Landing Layer
- Bronze Layer
- Silver Layer
- Gold Layer

---

# Cloud Storage & Governance Setup

The project uses Azure Data Lake Storage Gen2 as the centralized storage layer for the Data Lakehouse implementation.

## Configuration Steps

- Created Azure Data Lake Storage Gen2 account
- Configured Databricks Access Connector for secure integration
- Assigned Storage Blob Data Contributor permissions
- Created Storage Credential in Unity Catalog
- Configured External Location mapped to ADLS Gen2
- Enabled governed and secure external data access using Unity Catalog

---

# Landing Layer

The Landing Layer acts as the controlled ingestion point for raw source datasets.

## Purpose
- Store raw source files without transformations
- Preserve original historical data
- Enable controlled data ingestion into the Data Lakehouse

## Features
- Raw CSV and JSON source files
- Centralized storage in ADLS Gen2
- Source data preservation
- Batch-based ingestion support

---

# Bronze Layer

The Bronze Layer is the raw ingestion layer where source datasets are loaded into Delta tables.

## Features
- Schema enforcement
- Metadata tracking
- Audit column creation
- Delta Lake storage format
- Historical raw data preservation

## Processing Activities
- Read source files from Landing layer
- Apply predefined schemas
- Add ingestion timestamp and source metadata
- Write datasets into Delta tables

---

# Silver Layer

The Silver Layer is the trusted transformation layer where data quality and standardization operations are performed.

## Features
- Data cleansing
- Standardization
- Deduplication
- Flattening nested JSON structures
- Trusted and transformed datasets

## Processing Activities
- Remove unnecessary columns
- Standardize naming conventions
- Handle duplicate records
- Apply transformation logic
- Prepare datasets for analytical processing

---

# Gold Layer

The Gold Layer is the business-ready analytical layer optimized for reporting and analytics.

## Features
- Fact tables
- Dimension tables
- Aggregated analytical datasets
- BI-ready reporting outputs

## Business Use Cases
- Driver standings analysis
- Constructor standings analysis
- Historical Formula 1 analytics
- Reporting and dashboard integration

---

# Incremental Processing Framework

The architecture includes a production-style incremental processing framework for efficient ETL execution.

## Features
- Batch ID-based processing
- Batch control table
- Incremental merge/upsert logic
- Process status tracking
- Automated workflow execution

## Benefits
- Faster execution
- Reduced processing time
- Efficient resource utilization
- Scalable pipeline architecture

---

# Workflow Orchestration

Databricks Lakeflow Jobs are used for orchestrating the complete ETL workflow.

## Capabilities
- Automated pipeline execution
- Dependency management
- Retry handling
- Monitoring and alerting
- Scheduled execution

---

# Delta Lake Implementation

Delta Lake is implemented across Bronze, Silver, and Gold layers to provide:

- ACID transactions
- Schema enforcement
- Time travel support
- Reliable incremental processing
- Improved performance and scalability

---

# Unity Catalog Implementation

Unity Catalog is used for centralized governance and secure data management.

## Capabilities
- Centralized schema management
- External data access control
- Storage credential management
- External location configuration
- Secure table organization

---

# Technologies Used

- Azure Databricks
- PySpark
- Spark SQL
- Delta Lake
- Unity Catalog
- Azure Data Lake Storage Gen2 (ADLS Gen2)
- Databricks Access Connector
- Lakeflow Jobs
- Python
