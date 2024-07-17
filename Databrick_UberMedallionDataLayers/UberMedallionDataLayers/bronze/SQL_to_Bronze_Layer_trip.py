# Databricks notebook source
# SQL Server information
jdbcHostname = "sqlserveruberrideanalytics.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "sqldatabase_uberrideanalytics"
sqlUsername = dbutils.secrets.get(scope="sql_scope", key="sqlUsername")
sqlPassword = dbutils.secrets.get(scope="sql_scope", key="sqlPassword")

jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}"

connectionProperties = {
  "user" : sqlUsername,
  "password" : sqlPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Reading SQL Server table 
df_trip = spark.read.jdbc(url=jdbcUrl, table="dbo.Trip_Transaction_Table", properties=connectionProperties)
display(df_trip)


# COMMAND ----------

# ADLS Gen2 Connection Information
storage_account_name = dbutils.secrets.get(scope="storage_account", key="storage_username")
storage_account_key = dbutils.secrets.get(scope="storage_account", key="storage_password")
container_name = "bronze"
Trip_Transaction_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Trip_Transaction_delta"

# Spark Configuration Settings
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)


# COMMAND ----------

# df_trip.write.format("delta").mode("overwrite").save(Trip_Transaction_path)

df_trip.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(Trip_Transaction_path)

# COMMAND ----------

spark.read.load(Trip_Transaction_path).display()