# Databricks notebook source
# MAGIC %md
# MAGIC #Create a Data dimension

# COMMAND ----------

# MAGIC %md
# MAGIC ### Authentication

# COMMAND ----------

# import pyspark.sql.functions as F
# from random import randint
from pyspark.sql.types import * 
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth, date_format, dayofyear, weekofyear, to_date

# COMMAND ----------

# ADLS Gen2 connection information
storage_account_name = dbutils.secrets.get(scope="storage_account", key="storage_username")
storage_account_key = dbutils.secrets.get(scope="storage_account", key="storage_password")
container_name = "silver"
Trip_Transaction_silver_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Trip_Transaction_silverzone"
# Ride_Rating_silver_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Ride_Rating_silver"

# Spark Configuration Settings
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)


# COMMAND ----------

df_trip = spark.read.format("delta").load(Trip_Transaction_silver_path)
display(df_trip)

# COMMAND ----------

# 'trip_start_timestamp' sütununu datetime türüne çevir
df_trip = df_trip.withColumn("trip_start_timestamp", to_timestamp(col("trip_start_timestamp")))

# Tarihle ilgili yeni sütunlar ekle
df_trip = df_trip.withColumn("year", year(col("trip_start_timestamp"))) \
                 .withColumn("month", month(col("trip_start_timestamp"))) \
                 .withColumn("day", dayofmonth(col("trip_start_timestamp"))) \
                 .withColumn("date", date_format(col("trip_start_timestamp"), "yyyy-MM-dd")) \
                 .withColumn("day_of_the_year", dayofyear(col("trip_start_timestamp"))) \
                 .withColumn("week_of_the_year", weekofyear(col("trip_start_timestamp")))

# 'date' sütununu DateType'a çevir
df_trip = df_trip.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

# Değişiklikleri göstermek için DataFrame'i görselleştir
display(df_trip)

# COMMAND ----------

# Sadece belirli sütunları içeren yeni bir DataFrame oluştur
df_date = df_trip.select("trip_id", "year", "month", "day", "date", "day_of_the_year", "week_of_the_year")

# Yeni DataFrame'i göster
display(df_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Gold Zone

# COMMAND ----------

# container_name = "gold"
# Date_dimension_for_goldzone_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Date_dimension"

# COMMAND ----------

# df_date.repartition(1).write.mode("append").save(Date_dimension_for_goldzone_path)

# COMMAND ----------

# spark.read.load(Date_dimension_for_goldzone_path).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save as Table in Databricks Gold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold;

# COMMAND ----------

df_date.write.mode("overwrite").saveAsTable("gold.date_dimension")

# COMMAND ----------

date_dimension = spark.sql("SELECT * FROM gold.date_dimension")
display(date_dimension)