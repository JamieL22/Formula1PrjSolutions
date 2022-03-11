-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW most_dominant_teams
AS (
SELECT race_year, team, COUNT(
CASE WHEN position = 1 then 1 ELSE NULL END) as total_wins, SUM(points)
FROM f1_presentation.calculated_race_results
GROUP BY race_year, team
ORDER BY total_wins DESC, race_year DESC)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC html = "<h1>Most Dominant Teams</h1>"
-- MAGIC 
-- MAGIC displayHTML(html)

-- COMMAND ----------

SELECT * FROM most_dominant_teams

-- COMMAND ----------

SELECT * FROM most_dominant_teams

-- COMMAND ----------


