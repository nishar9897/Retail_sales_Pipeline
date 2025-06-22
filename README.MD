# Retail Sales Data Pipeline - GCP Project

## Overview

This project demonstrates an end-to-end retail sales data pipeline implemented on Google Cloud Platform (GCP). The pipeline handles data ingestion, transformation, and loading into a PostgreSQL instance hosted on Cloud SQL.

### Data Sources
- GCS Bucket (CSV) – Products and Customers
- Public API (JSON) – Sales
- On-Prem PostgreSQL – Additional raw tables (if required)

## Data Processing

- Used Apache Beam with Dataflow to read and clean data
- Applied transformations such as null removal, schema standardization, and ingestion timestamping
- Cleaned files are stored in the Silver Layer (GCS) in JSON format

## Automation Logic

To transition from a batch-tested manual setup to a fully automated pipeline, the following GCP services were utilized:

### Cloud Scheduler
Triggers the pipeline at predefined intervals (e.g., every morning at 8 AM) to begin ingestion from the source into the Bronze layer.

### Cloud Function 1
Activated after ingestion completion. It verifies file presence and publishes a Pub/Sub message to initiate the cleaning stage.

### Pub/Sub
Acts as an event messaging system between stages, triggering the appropriate downstream processes like cleaning or loading.

### Data Cleaning Job
Triggered by Pub/Sub, this Dataflow job performs cleansing using Apache Beam and writes the output to the Silver layer.

### Cloud Function 2
Monitors for completion of cleaned data in the Silver layer and publishes another Pub/Sub message to initiate loading.

### Cloud SQL Loader
Reads cleaned JSON files from GCS and inserts records into a Cloud SQL (PostgreSQL) database using Apache Beam's ParDo with psycopg2.

### Sequential Flow Summary

1. Cloud Scheduler triggers ingestion
2. Cloud Function 1 validates ingestion and publishes to Pub/Sub
3. Pub/Sub triggers the data cleaning pipeline
4. Cloud Function 2 validates Silver layer and publishes to Pub/Sub
5. Pub/Sub triggers the data loading pipeline into Cloud SQL

Each stage is conditionally triggered after successful completion of the previous stage, ensuring reliability, consistency, and scalability across the pipeline.

---

This project serves as a real-world demonstration of how to automate and manage data pipelines effectively using native GCP services and Apache Beam.
