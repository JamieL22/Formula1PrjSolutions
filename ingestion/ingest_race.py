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

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True), 
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), False), 
    StructField("name", StringType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read.option("header", True).schema(races_schema).csv(f"{raw_folder_path}/{file_date}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

races_filtered = races_df.select(col("raceId").alias("race_id"), col("year"), col("round"), col("circuitId").alias("circuit_id"),
                                col("name"), col("date"), col("time"))

# COMMAND ----------


display(races_filtered)

# COMMAND ----------

races_transformed = races_filtered.withColumn("ingestion_date", current_timestamp())

races_transformed = races_transformed.withColumn("race_timestamp",\
                                                 to_timestamp(\
                                                              concat(\
                                                                     col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss")) \
.withColumn("source", lit(data_source)).\
withColumn("file_date", lit(file_date))

# COMMAND ----------


display(races_transformed)

# COMMAND ----------

races_transformed.printSchema()

# COMMAND ----------

races_final = races_transformed.select(col("race_id"), col("round"), \
                                                         col("circuit_id"),
                                                         col("name"),
                                                         col("ingestion_date"),
                                                         col("race_timestamp"),
                                       col("year").alias("race_year"),\
                                                  col("source"),\
                                      col("file_date"))

# COMMAND ----------

display(races_final)

# COMMAND ----------

races_final.write.mode("overwrite").partitionBy("race_year").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

races_final.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_folder_path}/{file_date}/races")
