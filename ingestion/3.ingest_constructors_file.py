# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "testing")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_schema = '''constructorId INT, name STRING, nationality STRING, constructorRef STRING, url STRING'''

# COMMAND ----------

constructor_df = spark.read \
.schema(constructor_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Drop url Columns

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

display(constructor_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

constructor_final_df = constructor_dropped_df \
    .withColumnRenamed('constructorId', 'constructor_id') \
    .withColumnRenamed('constructorRef', 'constructor_ref') \
    .withColumn('ingestion_date', current_timestamp()) \
    .withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

constructor_final_df.write.mode('overwrite') \
.format("delta") \
.saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/fourmla1dl/processed/constructors

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors;