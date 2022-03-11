# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("file_date", "2021-03-21")

file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

races = spark.read.parquet(f"{processed_folder_path}/races")\
.withColumnRenamed("name", "race_name")\
.withColumnRenamed("year", "race_year")\
.withColumnRenamed("race_timestamp", "race_date")

drivers = spark.read.parquet(f"{processed_folder_path}/drivers")\
.withColumnRenamed("number", "driver_number")\
.withColumnRenamed("name", "driver_name")\
.withColumnRenamed("nationality", "driver_nationality")

circuits = spark.read.parquet(f"{processed_folder_path}/circuits")\
.withColumnRenamed("name", "circuits_name")\
.withColumnRenamed("location", "circuit_location")

constructors = spark.read.parquet(f"{processed_folder_path}/constructors")\
.withColumnRenamed("name", "constructor_name")

results = spark.read.parquet(f"{processed_folder_path}/results")\
.withColumnRenamed("time", "race_time")

# COMMAND ----------

race_results = results\
.join(constructors,results["constructor_id"] == constructors["constructor_id"], "inner")\
.join(drivers, results["driver_id"] == drivers["driver_id"], "inner")\
.join(races, results["race_id"] == races["race_id"], "inner")\
.join(circuits, races["circuit_id"] == circuits["circuit_id"], "inner" )

# COMMAND ----------

display(race_results)

# COMMAND ----------

race_results_final = race_results.select(\
                                         "race_year",
                                         "race_name",
                                         "race_date",
                                         "circuit_location",
                                         "driver_name",
                                         "driver_number",
                                         "driver_nationality",
                                         "constructor_name",
                                         "grid",
                                         "fastest_lap",
                                         "position",
                                         "race_time",
                                         "points"
                                        )\
.withColumnRenamed("constructor_name", "team")\
.withColumn("created_date", current_timestamp() )

# COMMAND ----------


display(race_results_final)

# COMMAND ----------

abu_dhabi_2020 = race_results_final.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix' ")

# COMMAND ----------

display(abu_dhabi_2020.orderBy("points", ascending=False))

# COMMAND ----------

race_results_final.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_final.write.mode("overwrite").format("parquet").\
saveAsTable("f1_presentation.race_results")
