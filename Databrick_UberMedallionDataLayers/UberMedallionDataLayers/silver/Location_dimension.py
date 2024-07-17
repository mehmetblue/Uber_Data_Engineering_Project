# Databricks notebook source
import pyspark.sql.functions as F
from random import randint
from pyspark.sql.types import * 

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

df_trip = spark.read.format("delta").load(Trip_Transaction_silver_path)
display(df_trip)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a new DataFrame

# COMMAND ----------

df_location_dimension = df_trip.select("trip_id", "source_location_address1", "source_city", "source_province_state", "source_country", "destination_location_address1", "destination_city", "destination_province_state","destination_country")
display(df_location_dimension)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Gold Zone

# COMMAND ----------

# container_name = "gold"
# location_dimension_for_goldzone_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Location_dimension"

# COMMAND ----------

# df_location_dimension.repartition(1).write.mode("append").save(location_dimension_for_goldzone_path)

# COMMAND ----------

# spark.read.load(location_dimension_for_goldzone_path).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save as Table in Databricks Gold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold;

# COMMAND ----------

# df_location_dimension_databricks=spark.read.format("delta").load(location_dimension_for_goldzone_path)
# display(df_location_dimension_databricks)

# COMMAND ----------

df_location_dimension.write.mode("overwrite").saveAsTable("gold.location_dimension")

# COMMAND ----------

location_dimension = spark.sql("SELECT * FROM gold.location_dimension")
display(location_dimension)