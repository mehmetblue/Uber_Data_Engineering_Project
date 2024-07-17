# Databricks notebook source
# MAGIC %md
# MAGIC # Create Driver Dimension

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import trim, col
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
# Ride_Rating_silver_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Ride_Rating_silverzone"

# Spark Configuration Settings
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading and Showing Data

# COMMAND ----------

df_trip = spark.read.format("delta").load(Trip_Transaction_silver_path)
df_trip

# COMMAND ----------

df_trip.take(1)

# COMMAND ----------

# df_ride = spark.read.format("delta").load(Ride_Rating_silver_path)
# df_ride

# COMMAND ----------

# df_ride.take(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### cleaning for df_ride

# COMMAND ----------

# df_ride = df_ride.withColumn("driver_id", trim(col("driver_id")))
# df_ride = df_ride.withColumn("driver_id", trim(col("driver_id")))

# COMMAND ----------

# df_ride.take(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Temp Table in Databricks

# COMMAND ----------

df_trip.createOrReplaceTempView("df_trip_temp_table")

# COMMAND ----------

# df_ride.createOrReplaceTempView("df_ride_temp_table") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selecting Columns for new DataFrames

# COMMAND ----------

df_driver_dimension=spark.sql('select distinct driver_id,driver_name from df_trip_temp_table')
df_driver_dimension

# COMMAND ----------

df_driver_dimension.take(2)

# COMMAND ----------

# df_ride_for_merge = spark.sql('select driver_id, driver_rating from df_ride_temp_table')
# df_ride_for_merge

# COMMAND ----------

# df_ride_for_merge.take(2)

# COMMAND ----------

df_driver_dimension.filter(df_driver_dimension["driver_id"] == "A1").show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Enrichment 

# COMMAND ----------

# creating random new Columns

def driver_age(driver_id):
    return randint(18,65)

def driver_gender(driver_id):
    if randint(0,2)==0:
        return 'F'
    else:
        return "M"
    

# COMMAND ----------

spark.udf.register("driver_age", driver_age,IntegerType())
spark.udf.register("driver_gender", driver_gender,StringType())

# COMMAND ----------

df_driver_dimension=df_driver_dimension.withColumn("driver_age",F.expr("driver_age(driver_id)"))
df_driver_dimension=df_driver_dimension.withColumn("driver_gender",F.expr("driver_gender(driver_id)"))

# COMMAND ----------

df_driver_dimension.take(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge DataFrames

# COMMAND ----------

# df_driver_dimension = df_trip_for_merge.join(df_ride_for_merge, on='driver_id', how='inner')

# COMMAND ----------

# df_driver_dimension.take(4)

# COMMAND ----------

# display(df_driver_dimension)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Driver Dimension to Gold Zone

# COMMAND ----------

# container_name = "gold"
# driver_dimension_for_goldzone_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Driver_dimension"


# COMMAND ----------

# df_driver_dimension.repartition(1).write.mode("append").save(driver_dimension_for_goldzone_path)

# COMMAND ----------

# spark.read.load(driver_dimension_for_goldzone_path).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save as Table in Databricks Gold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold;

# COMMAND ----------

# df_driver_dimension_for_databricks=spark.read.format("delta").load(driver_dimension_for_goldzone_path)
# display(df_driver_dimension_for_databricks)

# COMMAND ----------

df_driver_dimension.write.mode("overwrite").saveAsTable("gold.driver_dimension")

# COMMAND ----------

driver_dimension = spark.sql("SELECT * FROM gold.driver_dimension")
display(driver_dimension)