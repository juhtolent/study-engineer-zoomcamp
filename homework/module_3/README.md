
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `dezoomcamp.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://data-club-study-2026/yellow_tripdata_2024-*.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE dezoomcamp.yellow_tripdata_non_partitioned AS
SELECT * FROM dezoomcamp.external_yellow_tripdata;


-- Question 1. Counting records
SELECT COUNT(*)
FROM dezoomcamp.yellow_tripdata_non_partitioned;


-- Question 2. Data read estimation
SELECT COUNT (DISTINCT (PULocationID))
FROM dezoomcamp.external_yellow_tripdata;

SELECT COUNT (DISTINCT (PULocationID))
FROM dezoomcamp.yellow_tripdata_non_partitioned;

-- Question 3. Understanding columnar storage
SELECT PULocationID
FROM dezoomcamp.yellow_tripdata_non_partitioned;

SELECT PULocationID, DOLocationID
FROM dezoomcamp.yellow_tripdata_non_partitioned;

-- Question 4. Counting zero fare trips
SELECT COUNT(fare_amount)
FROM dezoomcamp.yellow_tripdata_non_partitioned
WHERE fare_amount = 0;


-- Question 5. Partitioning and clustering
-- Create a partitioned table from external table
CREATE OR REPLACE TABLE dezoomcamp.yellow_tripdata_partitioned
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM dezoomcamp.external_yellow_tripdata;

-- Question 6. Partition benefits
SELECT DISTINCT (VendorID)
FROM dezoomcamp.yellow_tripdata_non_partitioned
WHERE tpep_dropoff_datetime >= '2024-03-01' AND tpep_dropoff_datetime <= '2024-03-15';

SELECT DISTINCT (VendorID)
FROM dezoomcamp.yellow_tripdata_partitioned
WHERE tpep_dropoff_datetime >= '2024-03-01' AND tpep_dropoff_datetime <= '2024-03-15';

-- Question 9. Understanding table scans
SELECT count(*) FROM dezoomcamp.yellow_tripdata_non_partitioned
