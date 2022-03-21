# Databricks notebook source
dbutils.notebook.run("ingest_circuits_file", 0, {"data_source": "Ergast Api", "file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("ingest_constructor", 0, {"data_source": "Ergast Api", "file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("ingest_drivers", 0, {"data_source": "Ergast Api", "file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("ingest_lap_times", 0, {"data_source": "Ergast Api", "file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("ingest_pit_stops", 0, {"data_source": "Ergast Api", "file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("ingest_qualifying", 0, {"data_source": "Ergast Api", "file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("ingest_race", 0, {"data_source": "Ergast Api", "file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("ingest_results", 0, {"data_source": "Ergast Api", "file_date": "2021-04-18"})

# COMMAND ----------


