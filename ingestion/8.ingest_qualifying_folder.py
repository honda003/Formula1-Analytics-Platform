# Databricks notebook source
from pyspark.sql.types import StructType, StructField,IntegerType, StringType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

dbutils.widgets.text("p_data_source", "testing")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

qualifying_schema = StructType(fields=[
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True),
])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiline", True) \
.json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

qualifying_df.count()

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

final_qualifying_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.qualify_id = src.qualify_id"
merge_delta_data(final_qualifying_df, "f1_processed", "qualifying", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# final_qualifying_df.write.mode("overwrite") \
# .format("parquet") \
# .saveAsTable("f1_processed.qualifying")

# COMMAND ----------

display(spark.read.format("delta").load(f"{processed_folder_path}/qualifying"))