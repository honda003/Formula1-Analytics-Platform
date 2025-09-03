# Databricks notebook source
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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_schema = StructType(fields=[
    StructField("constructorId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("grid", IntegerType(), True),
    StructField("laps", IntegerType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("raceId", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("resultId", IntegerType(), False),
    StructField("statusId", StringType(), True),
    StructField("time", StringType(), True)
])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

renamed_results_df = results_df \
.withColumnRenamed("resultId", "result_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("positionText", "position_text") \
.withColumnRenamed("positionOrder", "position_order") \
.withColumnRenamed("fastestLap", "fastest_lap") \
.withColumnRenamed("fastestLapTime", "fastest_lap_time") \
.withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

dropped_results_df = renamed_results_df.drop("statusId")

# COMMAND ----------

final_results_df = dropped_results_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# for race_id_list in final_results_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# final_results_df.write \
# .mode("append") \
# .partitionBy("race_id") \
# .format("parquet") \
# .saveAsTable("f1_processed.results")

# COMMAND ----------

final_results_df = select_columns(final_results_df, "race_id")

# COMMAND ----------

final_results_df.columns

# COMMAND ----------

# overwrite_partition(final_results_df, "f1_processed", "results", "race_id")

# COMMAND ----------

results_deduped_df = final_results_df.dropDuplicates(["race_id", "driver_id"])

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.result_id = src.result_id"
merge_delta_data(results_deduped_df, "f1_processed", "results", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, driver_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id, driver_id
# MAGIC HAVING COUNT(1) > 1
# MAGIC ORDER BY race_id, driver_id DESC;