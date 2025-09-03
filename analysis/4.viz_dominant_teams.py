# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_dominant_teams AS
# MAGIC SELECT RANK() OVER(ORDER BY AVG(calculated_points) DESC) team_rank,
# MAGIC team_name, COUNT(1) AS total_races, SUM(calculated_points) AS total_points,
# MAGIC AVG(calculated_points) AS avg_points
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC GROUP BY team_name
# MAGIC HAVING COUNT(1) >= 50
# MAGIC ORDER BY avg_points DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC team_name, race_year,
# MAGIC COUNT(1) AS total_races, SUM(calculated_points) AS total_points,
# MAGIC AVG(calculated_points) AS avg_points
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 10)
# MAGIC GROUP BY team_name, race_year
# MAGIC ORDER BY race_year DESC, avg_points DESC

# COMMAND ----------

