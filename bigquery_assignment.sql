-- Creating external table referring to gcs path
-- CREATE EXTERNAL TABLE IN BIGQUERY
CREATE OR REPLACE EXTERNAL TABLE `ny_taxi_data.green_tripdata_external`
OPTIONS (
  format = 'parquet',
  uris = ['gs://de_zoomcamp/2022_data/green_tripdata_2022-*.parquet']
);

-- CREATE NON PARTITIONED TABLE IN BIGQUERY
CREATE OR REPLACE TABLE `ny_taxi_data.green_tripdata_nonpartitioned`
AS SELECT * FROM `ny_taxi_data.green_tripdata_external`;

-- CREATE PARTITIONED TABLE IN BIGQUERY
CREATE OR REPLACE TABLE `ny_taxi_data.green_tripdata_partitioned`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS (
  SELECT * FROM `ny_taxi_data.green_tripdata_external`
);

-- GET DISTINCT PULocationID FROM THE EXTERNAL AND NON PARTITIONED TABLES
SELECT COUNT(DISTINCT(PULocationID)) FROM `ny_taxi_data.green_tripdata_external`;
SELECT COUNT(DISTINCT(PULocationID)) FROM `ny_taxi_data.green_tripdata_nonpartitioned`;

-- COUNT WHERE FARE_AMOUNT = 0
SELECT COUNT(*) FROM `ny_taxi_data.green_tripdata_external` WHERE fare_amount = 0;

-- GET DISTINCT PULocationID FROM THE PARTITIONED AND NON PARTITIONED TABLES
SELECT DISTINCT(PULocationID) FROM  `ny_taxi_data.green_tripdata_nonpartitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

SELECT DISTINCT(PULocationID) FROM  `ny_taxi_data.green_tripdata_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
