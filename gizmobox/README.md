# Gizmobox Project

The **Gizmobox Project** is a simulated end-to-end data engineering pipeline built using **Azure Databricks** and **ADLS Gen2**. It demonstrates how to ingest, process, and analyze large volumes of event data in a scalable, structured, and performant manner.

## 📚 Project Overview
## Architecture

This project implements the **Medallion Architecture**:

- **Bronze Layer**: Ingest raw JSON data into Delta tables with minimal transformations.
- **Silver Layer**: Clean and enrich data to create structured, analytics-ready datasets.
- **Gold Layer**: Perform aggregations and business-level summarizations for reporting and analysis.

Gizmobox is a fictional technology company that collects event data from users interacting with its digital platforms. This project builds a robust ETL pipeline to transform raw event data into meaningful business insights.

## There are total of 8 files that I have Extracted/Transformed and Loaded from bronze to silver to finally Gold. 
**01.set-up-Project_env notebook:**
- I have created a container in ADLS Gen2 that we want to access from databricks. gizmobox -> landing ->(has 2 folders) -> operational_data and external_data
- I have created Volumes on operational_data but NOT ON external_data folder
- Created external location to access this container named : gizmobox
- Created catalog named gizmobox
- Under this catalog, created 4 schemas: landing, bronze, silver and gold

**02.persisting_operational_data_into_bronze**
I have created a view to bring all the data from landing schema into the bronze schema for the following file:
1. customers : JSON
2. orders(text file with inconsistent data): JSON
3. memberships (unstructured files): IMAGES
4. addresses(csv - delimiter:tab) - Have used read_files() option instead of the classic select option

**03.persisting_external_data_into_bronze**
Since I have not created unity catalog volume on external_data folder, I CAN use external tables here - I can use the abfss protocal to access the data, as I have already created the external location on the gizmobox container itself
5. processes payments file into bronze schema
- Created external table for payments file in bronze
- have demonstrated the effect of adding/updating/deleting files
- when we drop an external table, only metadata gets deleted from the unity catalog. The data files remains in the storage account

**04.persisting_Azure_SQL_data_into_bronze**
 via JDBC in hive metastore, creating external table in databricks for quering the data
 6. processes refunds file into bronze schema
 - created bronze schema in Hive Metastore
 - created the external table

**05.extract_data_using_pyspark**
   Here I have demonstrated the data extraction using pyspark and spark SQL into a dataframe

**06.Data_Profiling**
 In Databricks, profiling typically refers to the process of analyzing a dataset to understand its structure, content, quality, and distribution. It is often the first step in data exploration and cleaning. Here I have demonstarted ways to profile the data in Databricks:
> Using Notebook User Interface
> Using DBUtils
> Profile data manually - using SQL statements

**07.Transforming_Customer_data**
Trasformmed Customer Data
> Removed records with NULL customer_id
> Removed duplicate records
> Removed duplicates based on created_timestamp with CTE
> CAST the columns to correct the DATATYPES
> Transformed data to silver schema using CTAS

**08.Transform_Payments_data**
Trasformmed Payments Data
> Extracted Date and Time from Payment_timestamp and create 2 new columns "payments_date" and "payment_time" using DATE_FORMAT()
> Mapped payment_status to contain descriptive values ( 1-Success, 2-Pending, 3-Cancelled, 4-Failed) using CASE STATEMENT
> wrote the transformed data to silver schema using CTAS

**09.Transform_Refunds_data**
> Extracted specific portion of the string from refund_reason using SPLIT()
> Extracted date and time from the refund_timestamp
> wrote transformed data to the silver schema in hive meta store

**10.Transform_memberships_data**
> Extract customer_id from the file path using REGEXP_EXTRACT()
> Write transformed data to the silver schema using CTAS

**11.Transform_Addresses_data**
> Created one record for each customer with 2 sets of address columns, 1 for shipping and 1 for billing address - using PIVOT()
> wrote transformed data to the silver schema

**12.1.Transform_Orders_data_Convert_String_to_JSON**
> Pre-processed the JSON string to fix the data Quality issues using REGEXP_REPLACE()
> Transformed JSON string to JSON object using SCHEMA_OF_JSON() and FROM_JSON()
> wrote transformed data to the silver schema

**12.2.Transform_orders_data_Explode_Arrays**
> Access elements from JSON object
> Deduplicate Array Elements using ARRAY_DISTINCT()
> Explode Arrays using EXPLODE()
> wrote the transformed Data to Silver Schema

**13.Joining_Customer_Addresses**
> Joined Customer data with address data to create a customer_address table which contains the address of each customer on the same record
> Persisted data into GOLD schema

**14.Agreegating_Monthly_Order_Summary**
> Created monthly order summer for each customer. Produced the following summary per month:
  - Total Orders
  - Total items bought
  - Total amount spent
> Created table in GOLD schema using CTAS
