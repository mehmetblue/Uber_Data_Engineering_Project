# Databricks notebook source
# MAGIC %md
# MAGIC # Create Trip Transaction  Fact

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### Authentication

# COMMAND ----------

# ADLS Gen2 connection information
storage_account_name = dbutils.secrets.get(scope="storage_account", key="storage_username")
storage_account_key = dbutils.secrets.get(scope="storage_account", key="storage_password")
container_name = "silver"
Trip_Transaction_silver_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Trip_Transaction_silverzone"

# Spark Configuration Settings
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading data

# COMMAND ----------

df_trip=spark.read.format("delta").load(Trip_Transaction_silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Select Specific Columns for New DataFrame

# COMMAND ----------

df_trip_for_gold_zone=df_trip.select("trip_id","trip_start_timestamp","trip_end_timestamp","trip_status","total_fare","total_distance","delay_start_time_mins","customer_id",'driver_id')

# COMMAND ----------

# df_trip_for_gold_zone.take(1)
display(df_trip_for_gold_zone)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Enrichment

# COMMAND ----------

df_trip_for_gold_zone=df_trip_for_gold_zone.withColumn("Trip_Date",F.expr("to_date(trip_start_timestamp)"))

# COMMAND ----------

df_trip_for_gold_zone.take(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Gold Zone

# COMMAND ----------

# container_name = "gold"
# Trip_Transactions_gold_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Trip_Transactions_Fact"

# COMMAND ----------

# df_trip_for_gold_zone.repartition(1).write.mode("append").save(Trip_Transactions_gold_path)

# COMMAND ----------

# spark.read.load(Trip_Transactions_gold_path).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save as Table in Databricks Gold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold;

# COMMAND ----------

# df_trip_for_databricks=spark.read.format("delta").load(Trip_Transactions_gold_path)
# display(df_trip_for_databricks)

# COMMAND ----------

df_trip_for_gold_zone.write.mode("overwrite").saveAsTable("gold.trip_transaction_fact")

# COMMAND ----------

trip_transaction_fact = spark.sql("SELECT * FROM gold.trip_transaction_fact")
display(trip_transaction_fact)