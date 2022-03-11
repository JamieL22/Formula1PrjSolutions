# Databricks notebook source
raw_folder_path = "abfss://raw@testdlcap.dfs.core.windows.net"
processed_folder_path = "abfss://processed@testdlcap.dfs.core.windows.net"
presentation_folder_path = "abfss://presentation@testdlcap.dfs.core.windows.net"

# COMMAND ----------

setup = spark.conf.set(
    "fs.azure.account.key.testdlcap.dfs.core.windows.net",
    dbutils.secrets.get(scope="dl-scope",key="dbx-secret"))

# COMMAND ----------


