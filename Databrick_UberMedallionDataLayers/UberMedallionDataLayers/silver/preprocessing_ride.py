# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *

# COMMAND ----------

# ADLS Gen2 Connection Information
storage_account_name = dbutils.secrets.get(scope="storage_account", key="storage_username")
storage_account_key = dbutils.secrets.get(scope="storage_account", key="storage_password")
container_name = "bronze"
Ride_Rating_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Ride_Rating_delta"

# Spark Configuration Settings
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)


# COMMAND ----------

df_ride = spark.read.option("format",'delta').load(Ride_Rating_path)
display(df_ride)

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS ride_rating")
# df_ride.write.saveAsTable("Ride_Rating")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Silver Zone

# COMMAND ----------

container_name = "silver"
Ride_Rating_for_silverzone_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Ride_Rating_silverzone"


# COMMAND ----------

df_ride.repartition(1).write.mode("overwrite").save(Ride_Rating_for_silverzone_path)

# COMMAND ----------

df_ride_silver_zone = spark.read.option("format",'delta').load(Ride_Rating_for_silverzone_path)
display(df_ride_silver_zone)