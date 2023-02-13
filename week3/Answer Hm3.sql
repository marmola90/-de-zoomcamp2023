CREATE OR REPLACE EXTERNAL TABLE `de-zoomcampam.trips_data_all.external_fhv_tripdata` 
OPTIONS (
  format='CSV',
  uris=['gs://de_data_lake_de-zoomcampam/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

--- Q1
SELECT COUNT(1)
FROM `trips_data_all.fhv_data`;
--- R 43244696

--- Q2
SELECT COUNT(DISTINCT(Affiliated_base_number))
FROM `trips_data_all.fhv_data`; -- 317.94

SELECT COUNT(DISTINCT(Affiliated_base_number))
FROM `trips_data_all.external_fhv_tripdata`; -- 0

--- Q3
SELECT COUNT(1)
FROM `trips_data_all.fhv_data`
WHERE PUlocationID is null and DOlocationID is null; 
--- R 717748

--- Q4
CREATE OR REPLACE TABLE `de-zoomcampam.trips_data_all.fhv_tripdata_partClust` 
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS
SELECT * FROM `trips_data_all.fhv_tripdata`;

--- Q5
SELECT DISTINCT(Affiliated_base_number)
FROM `trips_data_all.fhv_data`
WHERE pickup_datetime BETWEEN '2019-03-01' and '2019-03-31';
--- 647.87
SELECT DISTINCT(Affiliated_base_number)
FROM `trips_data_all.fhv_tripdata_partClust`
WHERE pickup_datetime BETWEEN '2019-03-01' and '2019-03-31';
--- 23.05

