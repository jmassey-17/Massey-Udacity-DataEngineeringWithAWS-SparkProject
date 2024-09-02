# AWS and Spark Data Engineering Project

## Project Overview

This project is part of the Udacity Data Engineering Nanodegree, focusing on building a data pipeline using AWS services and Apache Spark. The pipeline processes accelerometer and step trainer data, demonstrating key data engineering concepts, including data ingestion, transformation, and storage.

## Objectives

- **Ingest Data:** Read raw data from a landing zone in AWS S3.
- **Transform Data:** Use Apache Spark to process and clean the data.
- **Store Data:** Output the processed data to trusted and curated zones in S3 for further analysis.

## Project Structure

- **data/**: Contains sample data files used for testing and development.
- **scripts/**:
  - `data_processing.py`: Python script using Spark to process raw data.
  - `sql_queries.sql`: SQL queries used for transforming and cleaning data.
- **notebooks/**: Jupyter notebooks for exploratory data analysis and prototyping.
- **output/**: Directory where processed data is stored after transformation.

## Technologies Used

- **Amazon Web Services (AWS)**: S3 for data storage.
- **Apache Spark**: Data processing and transformation.
- **Python**: Scripting and data manipulation.
- **SQL**: Data querying and aggregation.

## Setup and Installation

1. **Clone the repository:**
   '''git clone https://github.com/jmassey-17/Massey-Udacity-DataEngineeringWithAWS-SparkProject.git'''
2. **Configure AWS credentials:**
    Set up your AWS CLI and configure your credentials to access S3.
3. **Run the data processing script:**
   '''python scripts/data_processing.py'''
