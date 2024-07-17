# Databricks notebook source
# MAGIC %md
# MAGIC #Create a Payment Status dimension

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
# MAGIC ### Reading and Showing Datas

# COMMAND ----------

df_trip=spark.read.load(Trip_Transaction_silver_path)
df_trip

# COMMAND ----------

df_trip.take(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select Specific Columns for Payment DataFrame

# COMMAND ----------

df_payment=df_trip.select("trip_id","payment_method","payment_Status","trip_start_timestamp")
df_payment.take(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add a New Column for Due Date

# COMMAND ----------

df_payment=df_payment.withColumn("Due_Date",F.expr("to_date(trip_start_timestamp)+7"))
df_payment.take(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Gold Zone

# COMMAND ----------

# container_name = "gold"
# payment_status_dimension_for_goldzone_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Payment_status_dimension"

# COMMAND ----------

# df_payment.repartition(1).write.mode("append").save(payment_status_dimension_for_goldzone_path)

# COMMAND ----------

# spark.read.load(payment_status_dimension_for_goldzone_path).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save as Table in Databricks

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold;

# COMMAND ----------

# df_payment_for_databricks=spark.read.format("delta").load(payment_status_dimension_for_goldzone_path)
# display(df_payment_for_databricks)

# COMMAND ----------

df_payment.write.mode("overwrite").saveAsTable("gold.payment_status_dimension")

# COMMAND ----------

payment_status_dimension = spark.sql("SELECT * FROM gold.payment_status_dimension")
display(payment_status_dimension)