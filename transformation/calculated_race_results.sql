-- Databricks notebook source
USE f1_processed

-- COMMAND ----------


CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
USING parquet
AS
SELECT races.race_year, constructors.name team,
drivers.name driver_name, results.position, 
results.points, 11 - results.points calculated_points
  FROM results
  JOIN drivers ON results.driver_id = drivers.driver_id
  JOIN constructors ON results.constructor_id = constructors.constructor_id
  JOIN races ON results.race_id = races.race_id
  
WHERE results.points <= 10

-- COMMAND ----------


