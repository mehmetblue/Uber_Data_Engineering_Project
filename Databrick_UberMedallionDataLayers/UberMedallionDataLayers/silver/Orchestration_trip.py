# Databricks notebook source
# MAGIC %run /Workspace/UberMedallionDataLayers/bronze/SQL_to_Bronze_Layer_trip

# COMMAND ----------

# MAGIC %run /Workspace/UberMedallionDataLayers/silver/preprocessing_trip

# COMMAND ----------

# MAGIC %run /Workspace/UberMedallionDataLayers/silver/Customer_Dimension

# COMMAND ----------

# MAGIC %run /Workspace/UberMedallionDataLayers/silver/Date_Dimension

# COMMAND ----------

# MAGIC %run /Workspace/UberMedallionDataLayers/silver/Driver_Dimension

# COMMAND ----------

# MAGIC %run /Workspace/UberMedallionDataLayers/silver/Location_dimension

# COMMAND ----------

# MAGIC %run /Workspace/UberMedallionDataLayers/silver/Payment_status_dimension

# COMMAND ----------

# MAGIC %run /Workspace/UberMedallionDataLayers/silver/trip_transacation_fact