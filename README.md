﻿# Pyspark-ETL-pipeline-with-Task-scheduler
# NugaBank ETL Project

This project implements an ETL pipeline using PySpark to process and transform NugaBank’s customer transaction data for storage in a PostgreSQL database. The pipeline enables efficient data processing and storage, improving accessibility for analytics and reporting.

## Project Structure

The ETL pipeline includes:
- **Extraction**: Reads data from a CSV file into a PySpark DataFrame.
- **Transformation**: Cleans data and organizes it into normalized tables (`transaction`, `customer`, `employee`, and `fact_table`). Unique identifiers are assigned to each record.
- **Loading**: Inserts data into PostgreSQL tables to support efficient querying and analysis.

## Components

- **PySpark**: For data processing, cleaning, and transformations.
- **PostgreSQL**: Used as the target data warehouse.
- **psycopg2**: Manages the database connection and table creation.

## Prerequisites

- **Spark** and **PySpark**
- **PostgreSQL** server running locally
- **PostgreSQL JDBC driver** (`postgresql-42.7.4.jar`) in the project directory

## Setup

1. Ensure the path to the raw data CSV file (`dataset/rawdata/nuga_bank_transactions.csv`) is correct.
2. Update the PostgreSQL credentials in the `get_db_connection()` function.

## Steps to Run the ETL Pipeline

1. **Initialize PostgreSQL Tables**: Run `create_table()` to create or reset the target tables.
2. **Run the ETL Script**: Executes data extraction, cleaning, transformation, and loading into PostgreSQL.
3. **Completion**: A success message will log as `"Data has been loaded successfully."`

## Code Summary

### Data Extraction
Extracts raw transaction data from CSV into a PySpark DataFrame.

```python
nugabank_df = spark.read.csv(r'dataset/rawdata/nuga_bank_transactions.csv', header=True, inferSchema=True)
