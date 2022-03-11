# Databricks notebook source
# MAGIC %run "./configuration"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def include_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

# def re_arrange_partition_column(input_df, partition_column):
#     column_list = []
#     for column_name in input_df.schema.names:
#         if column_name != partition_column:
#             column_list.append(column_name)
#     column_list.append(partition_column)
#     output_df = input_df.select(column_list)
#     return output_df

# COMMAND ----------

# def overwrite_partition(input_df, db_name, table_name, partition_column, file_date, p_path = {processed_folder_path}):
#     output_df = re_arrange_partition_column(input_df, partition_column)
#     spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
#     if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
#         temp_df = output_df
#         temp_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
#         temp_df.write.mode("append").partitionBy("race_id").parquet(f"{processed_folder_path}/{file_date}/results")
#     else:
#         output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")
        
#         output_df.write.mode("overwrite").partitionBy(partition_column).parquet(f{})

# COMMAND ----------


