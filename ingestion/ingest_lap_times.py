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

lap_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
StructField("driverId", IntegerType(), True),
StructField("lap", IntegerType(), True),
StructField("position", IntegerType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)])

# COMMAND ----------

lap_df = spark.read.schema(lap_schema).csv(f"{raw_folder_path}/{file_date}/lap_times/lap_times_split*.csv")

# COMMAND ----------

display(lap_df)

# COMMAND ----------

lap_final = lap_df.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("driverId", "driver_id")\
.withColumn("ingestion_date", current_timestamp())\
.withColumn("source", lit(data_source))\
.withColumn("file_date", lit(file_date))

# COMMAND ----------

for race_id_list in lap_final.select("race_id").distinct().collect():
    if (spark._jsparkSession.catalog().tableExists("f1_processed.lap_times")):
        spark.sql(\
             f"ALTER TABLE f1_processed.lap_times DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

lap_final.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

lap_final = spark.sql("""
SELECT * FROM f1_processed.lap_times""")

# COMMAND ----------

lap_final.write.mode("append").partitionBy("race_id").parquet(f"{processed_folder_path}/{file_date}/lap_final")

# COMMAND ----------


