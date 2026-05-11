# Incremental Processing Framework

## Overview

The project implements a production-style incremental data processing framework using batch-based ingestion in Azure Databricks. The framework is designed to process only newly arrived or modified data instead of reprocessing the complete dataset, enabling scalable and efficient ETL execution.

The incremental framework is integrated with:
- Medallion Architecture
- Delta Lake
- Unity Catalog
- Databricks Lakeflow Jobs

---

# Framework Components

## Batch ID Tracking
Each incremental load is processed using a unique Batch ID to track individual execution cycles across Landing, Bronze, Silver, and Gold layers.

---

## Batch Control Table
A centralized batch control table is maintained to:
- Track pipeline executions
- Monitor processing status
- Store batch execution details
- Enable auditability and recovery

---

## Incremental Data Ingestion
New source files are ingested from the Landing layer into the Bronze layer using incremental batch processing logic.

The ingestion process includes:
- Schema enforcement
- Metadata tracking
- Audit column creation
- Delta table ingestion

---

## Merge / Upsert Logic
Delta Lake merge operations are implemented to:
- Insert new records
- Update existing records
- Prevent duplicate data processing
- Maintain reliable historical datasets

---

## Process Status Monitoring
Pipeline execution status is monitored throughout the workflow using:
- Batch status tracking
- Execution logging
- Workflow orchestration monitoring
- Retry and dependency handling

---

# Incremental Processing Workflow

1. New batch files arrive in the Landing layer
2. Batch ID is generated for the execution cycle
3. Source data is incrementally ingested into Bronze Delta tables
4. Silver layer transformations and standardization are applied
5. Gold layer fact and dimension tables are updated
6. Batch execution status is updated in the control table
7. Lakeflow Jobs orchestrate dependent pipeline execution

---

# Benefits

- Reduced processing time
- Faster ETL execution
- Efficient resource utilization
- Scalable pipeline architecture
- Reliable incremental data handling
- Production-style ETL implementation
- Improved maintainability and monitoring

---

# Technologies Used

- Azure Databricks
- PySpark
- Delta Lake
- Spark SQL
- Unity Catalog
- Lakeflow Jobs
- Azure Data Lake Storage Gen2 (ADLS Gen2)
