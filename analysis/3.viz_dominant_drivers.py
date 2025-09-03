# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_dominant_drivers AS
# MAGIC SELECT RANK() OVER(ORDER BY AVG(calculated_points) DESC) driver_rank,
# MAGIC driver_name, COUNT(1) AS total_races, SUM(calculated_points) AS total_points,
# MAGIC AVG(calculated_points) AS avg_points
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC GROUP BY driver_name
# MAGIC HAVING COUNT(1) >= 50
# MAGIC ORDER BY avg_points DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC driver_name, race_year,
# MAGIC COUNT(1) AS total_races, SUM(calculated_points) AS total_points,
# MAGIC AVG(calculated_points) AS avg_points
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
# MAGIC GROUP BY driver_name, race_year
# MAGIC ORDER BY race_year DESC, avg_points DESC

# COMMAND ----------

