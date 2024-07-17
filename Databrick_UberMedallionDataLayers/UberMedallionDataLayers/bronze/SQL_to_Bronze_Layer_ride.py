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
df_ride = spark.read.jdbc(url=jdbcUrl, table="dbo.Ride_Rating_Table", properties=connectionProperties)

# COMMAND ----------

display(df_ride)

# COMMAND ----------

# ADLS Gen2 Connection Information
storage_account_name = dbutils.secrets.get(scope="storage_account", key="storage_username")
storage_account_key = dbutils.secrets.get(scope="storage_account", key="storage_password")
container_name = "bronze"
Ride_Rating_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/Ride_Rating_delta"

# Spark Configuration Settings
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)


# COMMAND ----------

# df_ride.write.format("delta").mode("overwrite").save(Ride_Rating_path)

df_ride.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(Ride_Rating_path)