# energydatalake_aws
ETL pipelines on AWS to integrate ERCOT ISO electricity grid data with OpenMeteo weather data for real-time energy weather analytics.

# README: AWS ETL Data Engineering Project

## Table of Contents
1. [Background](#background)
2. [Data Architecture](#data-architecture)
3. [Initial Setup](#initial-setup)
4. [Ingest](#ingest)
5. [Load and Transform](#load-and-transform)
6. [Storage](#storage)
7. [Analysis](#analysis)
8. [Dashboard](#dashboard)
9. [Key Takeaways](#key-takeaways)
10. [Management](#management)

## Background

This project involves building an ETL pipeline on AWS to integrate and process 100GB of daily data from multiple sources. The solution employs various AWS services to ensure efficient data ingestion, transformation, storage, and analysis, while adhering to best practices for data governance.

### AWS Resources Utilized

| Resource             | Usage and Configuration                                                                            |
|----------------------|-----------------------------------------------------------------------------------------------------|
| **AWS Lambda**       | Serverless compute service used to run functions for data ingestion from various sources.         |
| **Amazon S3**        | Used as the staging area for raw and processed data, forming a data lake structure.               |
| **AWS Glue**         | Managed ETL service for transforming and preparing data for analytics.                             |
| **Amazon Redshift**  | Data warehouse solution used for querying processed data and supporting analytics workloads.       |
| **Amazon QuickSight**| Business intelligence service used for data visualization and dashboard creation.                  |
| **AWS CloudWatch**   | Monitoring and logging service to track the performance and status of AWS resources and functions. |
| **AWS IAM**          | Identity and Access Management for controlling access to AWS resources in compliance with security policies. |

## Data Architecture

The architecture consists of several layers, including data ingestion, processing, storage, and analysis. The following diagram illustrates the flow of data through the architecture:


## Initial Setup

1. **AWS Account**: Set up an AWS account with the necessary permissions to access and manage the resources.
2. **IAM Roles**: Create IAM roles for AWS Lambda and AWS Glue to allow them to access S3, Redshift, and other required services securely.
3. **S3 Buckets**: Create S3 buckets to store raw data, processed data, and results from ETL jobs.

## Ingest

Data ingestion is performed using AWS Lambda functions that fetch data from various sources, including APIs and databases.

- **Lambda Functions**: Implemented multiple Lambda functions to handle data extraction from each source.
- **Scheduling**: AWS EventBridge (formerly CloudWatch Events) schedules Lambda functions to run at defined intervals (e.g., hourly, daily).

## Load and Transform

The loading and transformation of data occur in several steps:

1. **Raw Data Storage**: Ingested data is first stored in a raw data folder in S3.
2. **ETL with AWS Glue**:
   - AWS Glue crawlers scan the raw data and create a schema.
   - Glue ETL jobs process and transform the data, cleaning and aggregating it as necessary.
   - Transformed data is then loaded into Amazon Redshift for further analysis.

## Storage

Data is stored using the following strategies:

- **Data Lake in S3**: Raw and processed data is stored in S3, providing a cost-effective solution for data storage.
- **Amazon Redshift**: Processed data is loaded into Redshift, which supports complex queries and analytics.

## Analysis

The analysis is performed in Amazon Redshift, allowing for fast query execution on large datasets.

- **SQL Queries**: Analysts can run SQL queries to derive insights from the processed data.
- **Performance Optimization**: Used Redshift's distribution styles and sort keys for optimized query performance.

## Dashboard

Data visualization is facilitated through Amazon QuickSight:

- **Data Connection**: QuickSight connects directly to Amazon Redshift to pull in the processed data for visualization.
- **Dashboard Creation**: Created interactive dashboards to visualize key metrics and data trends.

## Key Takeaways

- **Scalability**: Leveraging AWS services allows for a scalable solution that can handle increasing data volumes.
- **Cost Efficiency**: Using serverless architecture (Lambda, Glue) reduces operational costs.
- **Data Governance**: Implemented data governance practices through IAM for access control and CloudWatch for monitoring.

## Management

### Data Monitoring

- **AWS CloudWatch**: Monitors Lambda function execution and Glue job performance, providing alerts for failures or performance issues.
- **Data Quality Checks**: Implemented additional Lambda functions for data validation to ensure data quality before loading into Redshift.

### Security Practices

- **IAM Policies**: Fine-grained access control is enforced through IAM roles and policies to limit access to sensitive data.
