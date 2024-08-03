# Tokyo Olympic Azure Data Engineering Project

## Overview

The **Tokyo Olympic Azure Data Engineering Project** showcases a comprehensive data engineering pipeline using Azure services and Apache Spark. The project processes and analyzes data related to the Tokyo Olympics, integrating multiple Azure technologies for data ingestion, transformation, and analysis.

### Key Components

- **Data Ingestion**: 
  - **Azure Data Factory (ADF)** is used to gather data from various sources and store it in Azure Data Lake Storage Gen2 (ADLS Gen2).
  - Raw data includes CSV files with information on athletes, coaches, gender entries, medals, and teams.

- **Data Storage**: 
  - **Azure Data Lake Storage Gen2 (ADLS Gen2)** is utilized for scalable and secure storage of raw and processed data.
  - Data is organized into containers:
    - **`raw-data/`**: Contains raw CSV files.
    - **`cleansed-data/`**: Stores cleaned and transformed Parquet files.
    - **`confirmed-data/`**: Holds final datasets ready for analysis.

- **Data Transformation**: 
  - **Azure Databricks** leverages Apache Spark for big data processing, including data cleaning, transformation, and enrichment.
  - Data is cleaned by removing duplicates, handling missing values, and renaming columns for consistency.
  - Advanced transformations include joining datasets, summarizing data, and performing pivoting and unpivoting operations.

- **Data Analysis**:
  - **Azure Synapse Analytics** is used for further data analysis and reporting, providing powerful tools for querying and visualizing the processed data.

### Goals

- **Showcase Data Engineering Skills**: Demonstrates the use of Azure services and Databricks for a complete data engineering workflow.
- **Data Analysis**: Provides tools and transformations to analyze Olympic data, such as medal counts, participant statistics, and team details.
- **End-to-End Pipeline**: Illustrates the integration of data ingestion, storage, transformation, and analysis in a cloud environment.

### Technologies Used

- **Azure Data Factory (ADF)**: For data ingestion and integration, moving data from various sources to ADLS Gen2.
- **Azure Data Lake Storage Gen2 (ADLS Gen2)**: For scalable and secure storage of raw and transformed data.
- **Azure Databricks**: For big data processing and transformation using Apache Spark.
- **Azure Synapse Analytics**: For advanced data analysis and reporting.


Feel free to reach out with any questions or issues regarding the project.
