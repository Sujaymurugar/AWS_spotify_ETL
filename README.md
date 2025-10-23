
# AWS_spotify_ETL



## Introduction:

This project implements an end-to-end serverless data analytics pipeline on AWS to process, transform, and visualize Spotify data. The pipeline leverages AWS services such as S3, Glue, Athena, and QuickSight to enable automated data ingestion, transformation, querying, and reporting
## Technologies Used


1.Amazon S3 – Data storage (staging and warehouse layers)

2.AWS Glue – ETL processing and schema management

3.AWS Glue Crawler – Metadata creation in Glue Data Catalog

4.Amazon Athena – Serverless SQL query engine

5.Amazon QuickSight – Data visualization and reporting

6.Python / PySpark – Data transformation scripts
## Data Flow:
Spotify Data → S3 (Staging) → AWS Glue ETL → S3 (DW) → AWS Glue Crawler → Amazon Athena → Amazon QuickSight

## 1. Amazon S3 (Staging)

Raw Spotify data (e.g., album, track info, artists, and streams) is ingested and stored in a staging S3 bucket.
This serves as the data landing zone for all raw datasets.
## 2. AWS Glue ETL

Glue ETL jobs clean, transform, and enrich the raw Spotify data —

for example:  
Normalizing column names and data types
Joining with metadata such as artist or album information
The transformed and processed data is written to the S3 Data Warehouse (DW) bucket in optimized formats (e.g., Parquet) for better performance and cost efficiency.
## 3. AWS Glue Crawler

The crawler automatically scans the transformed data stored in S3 DW and updates the AWS Glue Data Catalog.
This process creates or updates schema and table definitions, enabling easy querying via Athena.
## 4. Amazon Athena

A serverless query engine that allows running SQL queries directly on data stored in S3.
Athena uses the Glue Data Catalog as its metadata layer to provide a structured view of the data.
## 5. Amazon QuickSight

A business intelligence (BI) and visualization tool connected to Athena.
Interactive dashboards and reports are built to analyze Spotify data trends
