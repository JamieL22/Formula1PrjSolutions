-- Databricks notebook source
SELECT *
FROM f1_presentation.calculated_race_results

-- COMMAND ----------

SELECT race_year, team, COUNT(
CASE WHEN position = 1 then 1 ELSE NULL END) as total_wins, SUM(points)
FROM f1_presentation.calculated_race_results
GROUP BY race_year, team
ORDER BY total_wins DESC, race_year DESC

-- COMMAND ----------


