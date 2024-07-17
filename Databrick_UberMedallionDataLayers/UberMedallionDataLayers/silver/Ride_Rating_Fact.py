# Databricks notebook source
# MAGIC %md
# MAGIC #Create a Ride Rating Fact

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
Ride_Rating_silver_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Ride_Rating_silverzone"

# Spark Configuration Settings
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading and showing data

# COMMAND ----------

df_ride = spark.read.load(Ride_Rating_silver_path)
display(df_ride)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Gold Zone

# COMMAND ----------

# container_name = "gold"
# Ride_Rating_gold_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Ride_Rating_Fact"

# COMMAND ----------

# df_ride.repartition(1).write.mode("append").save(Ride_Rating_gold_path)

# COMMAND ----------

# spark.read.load(Ride_Rating_gold_path).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save as Table in Databricks

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold;

# COMMAND ----------

# df_ride_for_databricks=spark.read.format("delta").load(Ride_Rating_gold_path)
# display(df_ride_for_databricks)

# COMMAND ----------

df_ride.write.mode("overwrite").saveAsTable("gold.ride_rating_fact")

# COMMAND ----------

ride_rating_fact = spark.sql("SELECT * FROM gold.ride_rating_fact")
display(ride_rating_fact)