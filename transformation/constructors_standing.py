# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("file_date", "2021-03-21")

file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/{file_date}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

constructors_df = race_results_df.groupBy("race_year",'team').\
agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("total_wins"))

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_window = Window.partitionBy("race_year").\
orderBy(desc("total_points"), desc("total_wins"))


constructors_final = constructors_df.withColumn("rank",\
                                               rank().over(constructors_window))

# COMMAND ----------

display(constructors_final.filter("race_year = 2020"))

# COMMAND ----------

constructors_final.write.format("overwrite").parquet(f"{presentation_folder_path}/{file_date}/constructors")

# COMMAND ----------

constructors_final.write.mode("append").format("parquet").\
saveAsTable("f1_presentation.constructors")

# COMMAND ----------


