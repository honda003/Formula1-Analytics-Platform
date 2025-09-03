# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingest_data(input_df):
    output_df = input_df.withColumn("ingest_date", current_timestamp())
    return output_df

# COMMAND ----------

def select_columns(df, partition_column):
    cols = [column for column in df.columns if column != partition_column]
    return df.select(*cols, partition_column)

# COMMAND ----------

def overwrite_partition(df, db_name, table_name, partition_column):
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        df.write \
        .mode("overwrite") \
        .insertInto(f"{db_name}.{table_name}")
    else:
        df.write \
        .mode("overwrite") \
        .partitionBy(partition_column) \
        .format("parquet") \
        .saveAsTable(f"{db_name}.{table_name}")



# COMMAND ----------

def df_column_to_list(input_df, column_name):
    df_row_list = input_df.select(column_name) \
        .distinct() \
        .collect()
    column_value_list = [row[column_name] for row in df_row_list]
    return column_value_list

# COMMAND ----------

def merge_delta_data(df, db_name, table_name, folder_path, merge_condition, partition_column):
    from delta.tables import DeltaTable
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias("tgt").merge(
            df.alias("src"),
            merge_condition) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    else:
        df.write \
        .mode("overwrite") \
        .partitionBy(partition_column) \
        .format("delta") \
        .saveAsTable(f"{db_name}.{table_name}")