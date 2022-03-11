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

qualifying_schema = StructType(fields=[
    StructField("qualifyId", IntegerType(), False),
StructField("raceId", IntegerType(), True),
StructField("driverId", IntegerType(), True),
StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
StructField("q1", StringType(), True),
StructField("q2", StringType(), True),
StructField("q3", StringType(), True),])
    

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option("multiline", True).json(f"{raw_folder_path}/{file_date}/qualifying/qualifying_split*.json")

# COMMAND ----------


display(qualifying_df)

# COMMAND ----------

qualifying_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id")\
.withColumnRenamed("driverId", "driver_id")\
.withColumn("ingestion_date", current_timestamp())\
.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("constructorId", "constructor_id")\
.withColumn("source", lit(data_source))\
.withColumn("file_date", lit(file_date))

# COMMAND ----------

for race_id_list in qualifying_df.select("race_id").distinct().collect():
    if (spark._jsparkSession.catalog().tableExists("f1_processed.qualifying")):
        spark.sql(\
             f"ALTER TABLE f1_processed.qualifying DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

qualifying_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

qualifying_df = spark.sql("""
SELECT * FROM f1_processed.qualifying""")

# COMMAND ----------

qualifying_df.write.mode("append").partitionBy("race_id").parquet(f"{processed_folder_path}/{file_date}/qualifying")
