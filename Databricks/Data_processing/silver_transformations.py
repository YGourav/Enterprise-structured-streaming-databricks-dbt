# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

import os
import sys

# COMMAND ----------

class transformations:

    def dedup(self, df: DataFrame, dedup_cols:List, lut:str):

        df = df.withColumn("dedupKey", concat(*dedup_cols))
        df = df.withColumn("countdedup", row_number().over(Window.partitionBy("dedupKey").orderBy(desc(lut))))
        df = df.filter(col('countdedup') == 1)
        df = df.drop('dedupKey', 'countdedup')
        return df
    
    def process_timestamp(self, df):

        df = df.withColumn("process_timestamp", current_timestamp())
        return df
    
    def upsert(self, df, key_cols, table, lut):
        merge_condition = " AND ".join([f"src.{k} = trg.{k}" for k in key_cols])
        dlt_obj = DeltaTable.forName(spark, f"pyspark_dbt.silver.{table}")
        dlt_obj.alias("trg").merge(df.alias("src"), merge_condition)\
                            .whenMatchedUpdateAll(condition = f"src.{lut} >= trg.{lut}")\
                            .whenNotMatchedInsertAll()\
                            .execute()
        return "Done"

# COMMAND ----------

current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
parent_dir

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Customers**

# COMMAND ----------

df_cust = spark.read.table("pyspark_dbt.bronze.customers")

# COMMAND ----------

df_cust = df_cust.withColumn("domain", split("email", "@")[1])

# COMMAND ----------

df_cust = df_cust.withColumn("phone_number", regexp_replace("phone_number", r"[^0-9]", ""))

# COMMAND ----------

df_cust = df_cust.withColumn("Full_Name", concat_ws(" ", col("first_name"), col("last_name")))
df_cust = df_cust.drop("first_name", "last_name")

# COMMAND ----------

# from setup.custom_utils import transformations

# COMMAND ----------

cust_obj = transformations()

cust_df_trans = cust_obj.dedup(df_cust, ['customer_id'], 'last_updated_timestamp')

# COMMAND ----------

df_cust = cust_obj.process_timestamp(cust_df_trans)

# COMMAND ----------

if not spark.catalog.tableExists("pyspark_dbt.silver.customers"):
    df_cust.write.format("delta")\
        .mode("append")\
        .saveAsTable("pyspark_dbt.silver.customers")
else:
    cust_obj.upsert(df_cust, ['customer_id'], 'customers', 'last_updated_timestamp')

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Drivers**

# COMMAND ----------

df_drivers = spark.read.table("pyspark_dbt.bronze.drivers")

# COMMAND ----------

df_drivers = df_drivers.withColumn("phone_number", regexp_replace("phone_number", r"[^0-9]", ""))

# COMMAND ----------

df_drivers = df_drivers.withColumn("Full_Name", concat_ws(" ", col("first_name"), col("last_name")))
df_drivers = df_drivers.drop("first_name", "last_name")

# COMMAND ----------

driver_obj = transformations()

# COMMAND ----------

df_driver = driver_obj.dedup(df_drivers, ['driver_id'], 'last_updated_timestamp')

# COMMAND ----------

df_drivers = driver_obj.process_timestamp(df_driver)

# COMMAND ----------

if not spark.catalog.tableExists("pyspark_dbt.silver.drivers"):
    df_drivers.write.format("delta")\
        .mode("append")\
        .saveAsTable("pyspark_dbt.silver.drivers")
else:
    driver_obj.upsert(df_drivers, ['driver_id'], 'drivers', 'last_updated_timestamp')

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Locations**

# COMMAND ----------

df_loc = spark.read.table("pyspark_dbt.bronze.locations")

# COMMAND ----------

loc_obj = transformations()

# COMMAND ----------

df_loc = loc_obj.dedup(df_loc, ['location_id'], 'last_updated_timestamp')
df_loc = loc_obj.process_timestamp(loc_df)

# COMMAND ----------

if not spark.catalog.tableExists("pyspark_dbt.silver.locations"):
    df_loc.write.format("delta")\
        .mode("append")\
        .saveAsTable("pyspark_dbt.silver.locations")
else:
    loc_obj.upsert(df_loc, ['location_id'], 'locations', 'last_updated_timestamp')

# COMMAND ----------

# MAGIC %md
# MAGIC ## **payments**

# COMMAND ----------

df_pay = spark.read.table("pyspark_dbt.bronze.payments")

# COMMAND ----------

df_pay = df_pay.withColumn("online_payment_status",
                           when( ((col('payment_method')=='Card') & (col('payment_status')=='Success')), "Online-Success")
                           .when( ((col('payment_method')=='Card') & (col('payment_status')=='Failed')), "Online-Failed")
                           .when( ((col('payment_method')=='Card') & (col('payment_status')=='Pending')), "Online-Pending")
                           .otherwise("Offline")
                )
display(df_pay)

# COMMAND ----------

pay_obj = transformations()

# COMMAND ----------

df_pay = pay_obj.dedup(df_pay, ['payment_id'], 'last_updated_timestamp')
df_pay = pay_obj.process_timestamp(df_pay)

# COMMAND ----------

if not spark.catalog.tableExists("pyspark_dbt.silver.payments"):
    df_pay.write.format("delta")\
        .mode("append")\
        .saveAsTable("pyspark_dbt.silver.payments")
else:
    pay_obj.upsert(df_pay, ['payment_id'], 'payments', 'last_updated_timestamp')

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Vehicles**

# COMMAND ----------

df_veh = spark.read.table("pyspark_dbt.bronze.vehicles")
display(df_veh)

# COMMAND ----------

df_veh = df_veh.withColumn("model", upper(col('model')))

# COMMAND ----------

veh_obj = transformations()

# COMMAND ----------

df_veh = veh_obj.dedup(df_veh, ['vehicle_id'], 'last_updated_timestamp')
df_veh = veh_obj.process_timestamp(df_veh)
if not spark.catalog.tableExists("pyspark_dbt.silver.vehicles"):
    df_veh.write.format("delta")\
        .mode("append")\
        .saveAsTable("pyspark_dbt.silver.vehicles") 
else:
    veh_obj.upsert(df_veh, ['vehicle_id'], 'vehicles', 'last_updated_timestamp')

# COMMAND ----------

