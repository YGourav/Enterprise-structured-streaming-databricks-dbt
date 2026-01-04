# Databricks notebook source
# df = spark.read.format("csv")\
#         .option("inferSchema", True)\
#         .option("header", True)\
#         .load("/Volumes/pyspark_dbt/source/source_data/customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Spark Streaming**

# COMMAND ----------

entities = ['customers', 'trips', 'locations', 'payments', 'vehicles', 'drivers']


# COMMAND ----------

for entity in entities: 

    df_batch = spark.read.csv(
    f"/Volumes/pyspark_dbt/source/source_data/{entity}",
    header=True,
    inferSchema=True
    )

    schema_entity = df_batch.schema

    df = spark.readStream.format("csv")\
        .option('header', 'true')\
        .schema(schema_entity)\
        .load(f"/Volumes/pyspark_dbt/source/source_data/{entity}/")

    df.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation", f"/Volumes/pyspark_dbt/bronze/checkpoint/{entity}/")\
        .trigger(once=True)\
        .toTable(f"pyspark_dbt.bronze.{entity}")
