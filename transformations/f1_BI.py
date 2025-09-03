# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE f1_presentation.aggregated_driver_yearly AS
# MAGIC SELECT
# MAGIC   r.race_year,
# MAGIC   d.driver_id,
# MAGIC   d.name AS driver_name,
# MAGIC   c.constructor_id AS team_id,
# MAGIC   c.name AS team_name,
# MAGIC   SUM(res.points) AS total_points,
# MAGIC   COUNT(CASE WHEN res.position_order = 1 THEN 1 END) AS total_wins
# MAGIC FROM f1_processed.results res
# MAGIC JOIN f1_processed.races r ON res.race_id = r.race_id
# MAGIC JOIN f1_processed.drivers d ON res.driver_id = d.driver_id
# MAGIC JOIN f1_processed.constructors c ON res.constructor_id = c.constructor_id
# MAGIC GROUP BY r.race_year, d.driver_id, d.name, c.constructor_id, c.name;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE f1_presentation.aggregated_team_yearly AS
# MAGIC SELECT
# MAGIC   r.race_year,
# MAGIC   c.constructor_id AS team_id,
# MAGIC   c.name AS team_name,
# MAGIC   SUM(res.points) AS total_points,
# MAGIC   COUNT(CASE WHEN res.position_order = 1 THEN 1 END) AS total_wins
# MAGIC FROM f1_processed.results res
# MAGIC JOIN f1_processed.races r ON res.race_id = r.race_id
# MAGIC JOIN f1_processed.constructors c ON res.constructor_id = c.constructor_id
# MAGIC GROUP BY r.race_year, c.constructor_id, c.name;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE f1_presentation.circuit_locations AS
# MAGIC SELECT
# MAGIC   circuit_id,
# MAGIC   name AS circuit_name,
# MAGIC   location,
# MAGIC   country,
# MAGIC   latitude,
# MAGIC   longitude
# MAGIC FROM f1_processed.circuits;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW f1_presentation.driver_summary_decade AS
# MAGIC SELECT
# MAGIC   FLOOR(r.race_year / 10) * 10 AS decade,
# MAGIC   d.driver_id,
# MAGIC   d.name AS driver_name,
# MAGIC   c.name AS team_name,
# MAGIC   SUM(res.points) AS total_points,
# MAGIC   COUNT(CASE WHEN res.position_order = 1 THEN 1 END) AS total_wins
# MAGIC FROM f1_processed.results res
# MAGIC JOIN f1_processed.races r ON res.race_id = r.race_id
# MAGIC JOIN f1_processed.drivers d ON res.driver_id = d.driver_id
# MAGIC JOIN f1_processed.constructors c ON res.constructor_id = c.constructor_id
# MAGIC GROUP BY FLOOR(r.race_year / 10) * 10, d.driver_id, d.name, c.name;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.aggregated_driver_yearly ORDER BY race_year, total_points DESC LIMIT 10;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.aggregated_team_yearly ORDER BY race_year, total_points DESC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.driver_summary_decade ORDER BY decade, total_points DESC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE f1_presentation.race_circuit_map AS
# MAGIC SELECT
# MAGIC   r.race_id,
# MAGIC   r.name AS race_name,
# MAGIC   r.race_year,
# MAGIC   c.circuit_id,
# MAGIC   c.name AS circuit_name,
# MAGIC   c.location,
# MAGIC   c.country
# MAGIC FROM f1_processed.races r
# MAGIC JOIN f1_processed.circuits c ON r.circuit_id = c.circuit_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_circuit_map

# COMMAND ----------

