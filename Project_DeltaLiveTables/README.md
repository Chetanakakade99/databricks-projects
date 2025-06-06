# Delta Live Tables (DLT) Project – Real-Time Data Pipeline with Databricks

## Project Overview

This project demonstrates the implementation of a **real-time data pipeline using Delta Live Tables (DLT)** on Databricks. The goal is to ingest, transform, and load streaming data through a medallion architecture (Bronze, Silver, and Gold layers), ensuring data quality, incremental processing, and monitoring.

The pipeline simulates real-world scenarios involving:
- Ingestion of raw streaming data
- Cleaning and enrichment of records
- Aggregations for analytics-ready data

##  Technologies Used

- **Databricks**
- **Delta Live Tables (DLT)**
- **PySpark**
- **Spark SQL**
- **Structured Streaming**
- **Auto Loader**
- **JSON/CSV input format**

## Architecture: Medallion Model

1. **Bronze Table**  
   Raw data is ingested using **Auto Loader**, enabling incremental file discovery and ingestion from cloud storage.  
   - Schema inference
   - Source tracking via checkpoints

2. **Silver Table**  
   Cleansed and transformed data.
   - Filtering invalid records
   - Casting columns to proper data types
   - Adding metadata like ingestion timestamps

3. **Gold Table**    
- Created Customer Order Summary in Gold Layer
- Joining the tables silver_customers, silver_addresses and silver_orders
- Retrieved the latest Address of the customer - since Customer had SCD type 2, it saves all the previous addresses as well
- Calculated the following values:
    total_orders
    total_items_ordered
    total_order_amount
