# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.testdlcap.dfs.core.windows.net",
    dbutils.secrets.get(scope="dl-scope",key="dbx-secret"))

# COMMAND ----------

# storage_account_name = "testdlcap"
# container_name = "raw"


# dbutils.fs.ls(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/")

