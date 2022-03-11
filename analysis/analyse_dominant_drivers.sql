-- Databricks notebook source
SELECT driver_name,
    COUNT(1) total_races,
    SUM(calculated_points) total_points,
    AVG(calculated_points) avg_points
  FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------


