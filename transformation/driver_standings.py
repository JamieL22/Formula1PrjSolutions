# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

driver_standings = race_results_df\
.groupby("race_year", "driver_name", "driver_nationality", "team")\
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(driver_standings)

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year")\
.orderBy(desc("wins"), desc("total_points"))


driver_standings_final = driver_standings\
.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

display(driver_standings_final.filter("race_year = 2020"))

# COMMAND ----------

driver_standings_final.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------

driver_standings_final.write.mode("overwrite").format("parquet").\
saveAsTable("f1_presentation.driver_standings")
