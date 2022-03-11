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

pit_stops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
StructField("driverId", IntegerType(), True),
StructField("stop", StringType(),True),
StructField("lap", IntegerType(), True),
StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)])

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema).option("multiline",True).json(f"{raw_folder_path}/{file_date}/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

pit_stops_final = pit_stops_df.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("driverId", "driver_id")\
.withColumn("ingestion_date", current_timestamp())\
.withColumn("source", lit(data_source))\
.withColumn("file_date", lit(file_date))

# COMMAND ----------

for race_id_list in pit_stops_final.select("race_id").distinct().collect():
    if (spark._jsparkSession.catalog().tableExists("f1_processed.pit_stops")):
        spark.sql(\
             f"ALTER TABLE f1_processed.pit_stops DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

pit_stops_final.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

pit_stops_final = spark.sql("""
SELECT * FROM f1_processed.pit_stops""")

# COMMAND ----------

pit_stops_final.write.mode("append").partitionBy("race_id").parquet(f"{processed_folder_path}/{file_date}/pit_stops")
