# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists pyspark_dbt

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog pyspark_dbt

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists pyspark_dbt.bronze;
# MAGIC create schema if not exists pyspark_dbt.silver;
# MAGIC create schema if not exists pyspark_dbt.gold;
# MAGIC create schema if not exists pyspark_dbt.source;