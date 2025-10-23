
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
Spotify Data → S3 (land) → AWS Glue ETL → S3 (final) → AWS Glue Crawler → Amazon Athena → Amazon QuickSight

<img width="1754" height="816" alt="image" src="https://github.com/user-attachments/assets/e3bfe70a-6614-44e1-a16d-631d3d9a5200" />



## 1. Amazon S3 (landing)

Raw Spotify data (e.g., album, track info, artists, and streams) is ingested and stored in a landing S3(SVMHA_land) bucket.
This serves as the data landing zone for all raw datasets.
<img width="1701" height="564" alt="image" src="https://github.com/user-attachments/assets/e6b3dde8-621c-4441-8616-990973e7a232" />

## 2. AWS Glue ETL

Glue ETL jobs clean, transform, and enrich the raw Spotify data —

for example:  
Normalizing column names and data types
Joining with metadata such as artist or album information
The transformed and processed data is written to the S3 Final(SVMHA_Final) bucket in optimized formats (e.g., Parquet) for better performance and cost efficiency.

<img width="1701" height="564" alt="image" src="https://github.com/user-attachments/assets/60dc2323-3826-4b3d-930a-55fe1a35e9db" />

## 3. AWS Glue Crawler

The crawler automatically scans the transformed data stored in S3 final and updates the AWS Glue Data Catalog.

<img width="1701" height="564" alt="image" src="https://github.com/user-attachments/assets/868fe151-fc76-424a-962a-fda9f8abea6d" />

This process creates or updates schema and table definitions, enabling easy querying via Athena.
## 4. Amazon Athena

A serverless query engine that allows running SQL queries directly on data stored in S3.
Athena uses the Glue Data Catalog as its metadata layer to provide a structured view of the data.

<img width="1717" height="687" alt="image" src="https://github.com/user-attachments/assets/76d40638-9718-4cc2-ae1b-3b897997246b" />

## 5. Amazon QuickSight

A business intelligence (BI) and visualization tool connected to Athena.
Interactive dashboards and reports are built to analyze Spotify data trends
