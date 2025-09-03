# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DateType
from pyspark.sql.functions import current_timestamp, lit, col, to_timestamp, concat

# COMMAND ----------

schema = StructType(
    [
        StructField("raceId", IntegerType(), True),
        StructField("year", IntegerType(), True),
        StructField("round", IntegerType(), True),
        StructField("circuitId", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("date", DateType(), True),
        StructField("time", StringType(), True),
        StructField("url", StringType(), True)
    ]
)

# COMMAND ----------

races_df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/races.csv") 

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_selected_df = races_df.select("raceId", "year", "round", "circuitId", "name", "date", "time")

# COMMAND ----------

renamed_races_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
    .withColumnRenamed("circuitId", "circuit_id") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

transformed_races_df = renamed_races_df.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

display(transformed_races_df)

# COMMAND ----------

final_races_df = transformed_races_df \
.drop("time", "date") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(final_races_df)

# COMMAND ----------

final_races_df.write.mode('overwrite') \
.format("delta") \
.saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/fourmla1dl/processed

# COMMAND ----------

df = spark.read.format("delta").load(f"{processed_folder_path}/races")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;