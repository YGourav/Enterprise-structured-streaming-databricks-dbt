from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import *

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
        dlt_obj = DeltaTable.forName(f"pyspark_dbt.silver.{table}")
        dlt_obj.alias("trg").merge(df.alias("src"), merge_condition)\
                            .whenMatchedUpdateAll(condition = "src.{lut} >= trg.{lut}")\
                            .whenNotMatchedInsertAll()\
                            .execute()
        return "Done"