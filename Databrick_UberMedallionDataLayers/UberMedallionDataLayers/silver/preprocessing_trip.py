# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

# COMMAND ----------

# ADLS Gen2 Connection Information
storage_account_name = dbutils.secrets.get(scope="storage_account", key="storage_username")
storage_account_key = dbutils.secrets.get(scope="storage_account", key="storage_password")
container_name = "bronze"
Trip_Transaction_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Trip_Transaction_delta"

# Spark Configuration Settings
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)


# COMMAND ----------

df_trip = spark.read.option("format",'delta').load(Trip_Transaction_path)
display(df_trip)

# COMMAND ----------

df_trip.count()

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS Trip_Transection")
# df_trip.write.saveAsTable("Trip_Transaction")

# COMMAND ----------

# %sql
# use catalog `hive_metastore`; select * from `default`.`trip_transection` limit 100;

# COMMAND ----------

# deltaTable = DeltaTable.forPath(spark, Trip_Transaction_path)

# COMMAND ----------

# %sql
# update Trip_Transection set driver_name ='Ram' where driver_id = 'A1';

# COMMAND ----------

# df_trip DataFrame'indeki tüm kolonları döngü içinde işleyerek metin tipinde olanları trim fonksiyonu ile güncelleme
for column in df_trip.columns:
    if isinstance(df_trip.schema[column].dataType, StringType):  # Kolonun metin tipinde olup olmadığını kontrol eder
        df_trip = df_trip.withColumn(column, trim(col(column)))  # Metin tipindeki kolonu trim ile güncelle

# Sonuçları Databricks'in display fonksiyonu ile göster
display(df_trip)

# COMMAND ----------

df_trip = df_trip.withColumn("driver_name", when(df_trip["driver_id"] == "A1", "Ram").otherwise(df_trip["driver_name"]))
display(df_trip)

# COMMAND ----------

# "Delhi" olanlar "New Delhi" olarak,
# "Salem" olanlar "Salem,Tamil Nadu" olarak güncelleniyor

# Hem 'source_city' hem de 'destination_city' sütunlarında değerleri güncelle

df_trip = df_trip \
    .withColumn("source_city", 
                when(col("source_city") == "Delhi", "New Delhi")
                .when(col("source_city") == "Salem", "Salem, Tamil Nadu")
                .otherwise(col("source_city"))) \
    .withColumn("destination_city", 
                when(col("destination_city") == "Delhi", "New Delhi")
                .when(col("destination_city") == "Salem", "Salem, Tamil Nadu")
                .otherwise(col("destination_city")))

# Sonuçları göster
display(df_trip)

# COMMAND ----------

# df_trip.source_city.value_counts()
df_trip.groupBy("source_city").count().show()

# COMMAND ----------

display(df_trip)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Silver Zone

# COMMAND ----------

container_name = "silver"
Trip_Transaction_for_silverzone_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Trip_Transaction_silverzone"

# COMMAND ----------

df_trip.repartition(1).write.mode("overwrite").save(Trip_Transaction_for_silverzone_path)

# COMMAND ----------

df_trip_silver_zone = spark.read.option("format",'delta').load(Trip_Transaction_for_silverzone_path)
display(Trip_Transaction_for_silverzone_path)