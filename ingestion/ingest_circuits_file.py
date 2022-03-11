# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("data_source", "")

data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("file_date", "2021-03-21")

file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col, lit

# COMMAND ----------

storage_account_name = "testdlcap"
container_name = "raw"


display(dbutils.fs.ls(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"))

# COMMAND ----------

circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True),



    
    
    
])

# COMMAND ----------

circuits = spark.read.option("header", True)\
.schema(circuits_schema)\
.csv(f"{raw_folder_path}/{file_date}/circuits.csv")

# COMMAND ----------


display(circuits)

# COMMAND ----------

circuits.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select Columns and Rename

# COMMAND ----------

circuits_selected_df = circuits.select(col("circuitId").alias("circuit_id"), col("circuitRef").alias("circuit_ref"), col("name"), col("location"), col("country"), col("lat").alias("latitude"), col("lng").alias("longitude"), col("alt").alias("altitude"))

# COMMAND ----------

circuits_selected_df = circuits_selected_df.withColumn("data_source", lit(data_source)).\
withColumn("file_date", lit(file_date))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Add timestamp for ingestion

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final = include_ingestion_date(circuits_selected_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Write to DL

# COMMAND ----------

circuits_final.write.mode("overwrite").parquet(f"{processed_folder_path}/{file_date}/circuits")

# COMMAND ----------

circuits_final.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------


