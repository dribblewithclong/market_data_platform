# Market Data Platform

## Project Overview

This project aims to build a modern data platform for storing and managing data from the Amazon market. The platform integrates various components to enable efficient data handling and analytics, including:

- **Data Lake:** MinIO
- **Open Table Format:** Apache Iceberg
- **OLAP Data Warehouse:** ClickHouse
- **Data Processing:** Apache Spark, Apache Kafka
- **Data Orchestration:** Apache Airflow

## Key Features

### 1. Web Scraping and Data Collection

- **Selenium:** Used for web scraping to collect customer reviews of products from Amazon.
- **SmartScout API:** Leveraged to retrieve data about product prices, ranks, and revenue.
- **Data Ingestion:** Raw data from Selenium and SmartScout API is ingested into the raw layer of MinIO.

### 2. Data Processing and Transformation

- **Apache Kafka:** Acts as an event-driven hub to receive batches of crawled data paths from MinIO.
- **Apache Spark:** Processes and transforms the data received from Kafka, loading the processed data into the serving layer of MinIO and ClickHouse.

### 3. Data Orchestration

- **Apache Airflow:** Automates and orchestrates the data pipeline, ensuring efficient scheduling, monitoring, and management of data processes across the pipeline.

### 4. Data Utilization

- **Data Scientists:** Use data stored in MinIO for exploration and building advanced machine learning models.
- **Business Intelligence:** ClickHouse serves as the primary data source powering dynamic BI dashboards.

## Components and Technologies

- **MinIO:** A high-performance object storage system used as the data lake.
- **Apache Iceberg:** An open table format designed for large-scale analytics.
- **ClickHouse:** A columnar OLAP database management system optimized for real-time queries and analytics.
- **Apache Spark:** A unified analytics engine for large-scale data processing.
- **Apache Kafka:** A distributed event streaming platform for high-throughput, low-latency data ingestion.
- **Apache Airflow:** A platform to programmatically author, schedule, and monitor workflows.

## Usage

1. **Data Ingestion:** Run the Selenium script to scrape Amazon reviews and use the SmartScout API to collect additional product data.
2. **Data Processing:** Kafka will automatically receive and forward the data path and Spark will load the data for processing.
3. **Data Storage:** The processed data will be stored in MinIO and ClickHouse for further analysis and BI reporting.
4. **Data Orchestration:** Use Airflow to schedule and monitor the data pipeline tasks.
