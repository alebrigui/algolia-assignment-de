# Algolia Project Assignment: S3 to PostgreSQL ETL Pipeline

## Overview

This assignment aims to implement a robust ETL pipeline that fetches Shopify configurations from an S3 bucket, processes them, and loads them into a PostgreSQL database.
The pipeline is constructed using Apache Airflow and contains several operations from data fetching, transformation, database table creation, data insertion, to final cleanup.

## Setup and running testss
**Initialize environment:**
```
make init 
```

**Run tests:**
```
make test
```

**For the remaining commands:**
```
make help
```

**To run Airflow:**
```
docker-compose up
```

## DAG Overview

The DAG, identified as `s3_to_postgres`, consists of the following tasks:

1. **s3_to_local**: Downloads data from the S3 bucket `alg-data-public` to the local filesystem.
2. **transform_csv**: Processes the CSV data using the function `transform_shopify_configurations` to ensure it adheres to required standards.
3. **create_table**: Creates the necessary PostgreSQL table for data ingestion.
4. **delete_old_data**: Purges any existing data corresponding to the current execution date to avoid duplication.
5. **load_data**: Loads the processed data into the PostgreSQL table `shopify_configs`.
6. **cleanup**: Deletes the downloaded and processed CSV files post-ingestion.

## Limitations & Potential Enhancements

### Current Limitations

1. **File Size Handling**: The current design might not efficiently handle very large files (>1GB) due to the entire dataset being loaded into memory during the transformation phase.
2. **Error Handling**: While there are retries set up, the pipeline might benefit from more advanced error-handling and alerting mechanisms.
3. **Data Type Handling**: 
   1. Some data types are not properly handled such as the array field that could be loaded as an `INTEGER[]` 
   2. Some casting is done during transformation, this could be better handled outside the transformation (e.g for field `nbr_metafields`)
4. **Data Latency**: The pipeline is scheduled to run at midnight for simplicity's sake this means that a file is processed with more than 22h of delay, we could adapt the scheduling to trigger it at 3am or use an S3FileSensor instead to reduce latency.
5. **Connection to S3**: Since the S3 Bucket is public the data, no credentials were used, in a production setting this should be avoided and an S3Hook should be used alongisde an Airflow Connection with the appropriate credentials.
6. **Security concerns**: Currently the docker-compose.yml file contains multiple secrets that should be better stored in a .env added to the .gitignore.
7. **Improve type hinting**: Not all args and variables are covered with type hinting, this could be an improvement.
8. **Schema Evolution**: The current script is not able to handle schema evolution, if the schema changes the pipeline might break. This could be handled by using a schema evolution tool, or coding logic that can detect schema changes and apply them. Using connectors (Airbyte, Singer) could also solve this issue.

### Suggested Enhancements

1. **Advanced Error Handling**: Implement logging and alerting based on error types, enabling faster troubleshooting.
2. **Data Type Adjustments**: Ensure all columns have the appropriate data types before writing to the database. This could involve more sophisticated type-checking and conversion functions.
3. **Improve testing**: The tests implemented cover the basic functionality of each operator, we could add more unit tests and add integration tests.

## Handling 500GB Data

Different approaches could be used to handle 500gb of daily data:

### Chunked processing to address memory bottleneck
For large files, the data transformation process can be done in chunks using pandas' `read_csv` with the `chunksize` parameter. This will allow processing large datasets without consuming excessive memory.

### Processing on the fly to address disk bottleneck
Alongside chunked processing, we could directly apply transformation and loading with the dataset's chunk in memory, this will allow us to avoid IO to disk. 

### Move to better-suited frameworks (e.g Spark)
Instead of processing the file as a whole, it could be partitioned, and each partition can be processed concurrently using a framework like Spark.
In this case Airflow is used only as the Orchestrator, and the actual processing would happen in a Spark Context inside the same or a different more powerful cluster (Glue, Databricks...)

### Use an ELT approach
Instead of transforming the data on Airflow workers, we can use Airflow to orchestrate a COPY command to cloud datawarehouse where the csvs would be loaded into STAGING schemas. Processing could then be inside of the warehouse using tools such as dbt.
