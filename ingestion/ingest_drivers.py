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

name_schema = StructType(fields=[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

drivers_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name",name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------


drivers_df = spark.read.schema(drivers_schema).json(f"{raw_folder_path}/{file_date}/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_filtered = drivers_df.withColumnRenamed("driverId", "driver_id").withColumnRenamed("driverRef", "driver_ref").withColumn("ingestion_date", current_timestamp()).withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))).withColumn("source", lit(data_source)).\
withColumn("file_daet", lit(file_date))

# COMMAND ----------

display(drivers_filtered)

# COMMAND ----------

drivers_final = drivers_filtered.drop(col("url"))

display(drivers_final)

# COMMAND ----------

drivers_final.write.mode("overwrite").format("parquet").\
saveAsTable("f1_processed.drivers")

# COMMAND ----------

drivers_final.write.mode("overwrite").parquet(f"{processed_folder_path}/{file_date}/drivers")
