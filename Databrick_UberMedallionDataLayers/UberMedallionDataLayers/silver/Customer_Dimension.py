# Databricks notebook source
# MAGIC %md
# MAGIC # Create Customer Dimension

# COMMAND ----------

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
# MAGIC ### Create Temp Table in Databricks

# COMMAND ----------

df_trip.createOrReplaceTempView("df_trip_temp_table") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selecting Some Columns for df

# COMMAND ----------

df_customer_dimension = spark.sql('select distinct customer_id,customer_name from df_trip_temp_table')
df_customer_dimension.take(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Enrichment

# COMMAND ----------

def customer_age(customer_id):
    return randint(18,65)
def customer_gender(customer_id):
    if customer_id%2==0:
        return 'M'
    else:
        return "F"
    

# COMMAND ----------

spark.udf.register("customer_age", customer_age,IntegerType())
spark.udf.register("customer_gender", customer_gender,StringType())

# COMMAND ----------

df_customer_dimension=df_customer_dimension.withColumn("customer_age",F.expr("customer_age(customer_id)"))
df_customer_dimension=df_customer_dimension.withColumn("customer_gender",F.expr("customer_gender(customer_id)"))

# COMMAND ----------

display(df_customer_dimension)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Gold Zone

# COMMAND ----------

# container_name = "gold"
# customer_dimension_for_goldzone_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Customer_dimension"

# COMMAND ----------

# df_customer_dimension.repartition(1).write.mode("append").save(customer_dimension_for_goldzone_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save as Table in Databricks Gold

# COMMAND ----------

# df_customer_dimension_databricks = spark.read.load(customer_dimension_for_goldzone_path)
# display(df_customer_dimension_databricks)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold;

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS gold.customer_dimension")
# df_customer_dimension.write.saveAsTable("gold.customer_dimension")

df_customer_dimension.write.mode("overwrite").saveAsTable("gold.customer_dimension")

# COMMAND ----------

customer_dimension = spark.sql("SELECT * FROM gold.customer_dimension")
display(customer_dimension)