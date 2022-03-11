# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("data_source", "Source")

data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("file_date", "2021-03-21")

file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@testdlcap.dfs.core.windows.net"))

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

constructor_df = spark.read.schema(constructors_schema).json(f"{raw_folder_path}/{file_date}/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

constructor_dropped_df = constructor_df.drop("url")

display(constructor_dropped_df)

# COMMAND ----------

constructors_final = constructor_dropped_df.withColumnRenamed("constructorRef", "constructor_red").withColumnRenamed("constructorId", "constructor_id").\
withColumn("file_date", lit(file_date))

# COMMAND ----------

constructors_final = include_ingestion_date(constructors_final)

# COMMAND ----------


constructors_final.write.mode("overwrite").format("parquet").\
saveAsTable("f1_processed.constructors")

# COMMAND ----------

constructors_final.write.mode("overwrite").parquet(f"{processed_folder_path}/{file_date}/constructors")
