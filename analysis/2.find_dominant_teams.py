# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.calculated_race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT team_name, SUM(points) AS total_points, COUNT(1) AS total_races
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC GROUP BY team_name
# MAGIC ORDER BY total_points DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT team_name, COUNT(1) AS total_races, SUM(calculated_points) AS total_points,
# MAGIC AVG(calculated_points) AS avg_points
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC GROUP BY team_name
# MAGIC HAVING COUNT(1) >= 50
# MAGIC ORDER BY avg_points DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT team_name, COUNT(1) AS total_races, SUM(calculated_points) AS total_points,
# MAGIC AVG(calculated_points) AS avg_points
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC WHERE race_year BETWEEN 2011 AND 2020
# MAGIC GROUP BY team_name
# MAGIC HAVING COUNT(1) >= 50
# MAGIC ORDER BY avg_points DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT team_name, COUNT(1) AS total_races, SUM(calculated_points) AS total_points,
# MAGIC AVG(calculated_points) AS avg_points
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC WHERE race_year BETWEEN 2001 AND 2010
# MAGIC GROUP BY team_name
# MAGIC HAVING COUNT(1) >= 50
# MAGIC ORDER BY avg_points DESC

# COMMAND ----------

