# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.circuits;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.circuits(
# MAGIC   circuitId INT,
# MAGIC   circuitRef STRING,
# MAGIC   name STRING,
# MAGIC   location STRING,
# MAGIC   country STRING,
# MAGIC   lat DOUBLE,
# MAGIC   lng DOUBLE,
# MAGIC   alt INT,
# MAGIC   url STRING)
# MAGIC   USING csv
# MAGIC   OPTIONS (
# MAGIC     path "/mnt/fourmla1dl/raw/circuits.csv", header true)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.races(
# MAGIC   raceId INT,
# MAGIC   year INT,
# MAGIC   round INT,
# MAGIC   circuitId INT,
# MAGIC   name STRING,
# MAGIC   date DATE,
# MAGIC   time STRING,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path "/mnt/fourmla1dl/raw/races.csv", header true)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.races

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.constructors;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.constructors(
# MAGIC   constructorId INT,
# MAGIC   constructorRef STRING,
# MAGIC   name STRING,
# MAGIC   nationality STRING,
# MAGIC   url STRING)
# MAGIC   USING JSON
# MAGIC   OPTIONS(path "/mnt/fourmla1dl/raw/constructors.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.constructors

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.drivers;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.drivers(
# MAGIC   driverId INT,
# MAGIC   driverRef STRING,
# MAGIC   number INT,
# MAGIC   code STRING,
# MAGIC   name STRUCT<forename: STRING, surname: STRING>,
# MAGIC   dob DATE,
# MAGIC   nationality STRING,
# MAGIC   url STRING)
# MAGIC   USING JSON
# MAGIC   OPTIONS(path "/mnt/fourmla1dl/raw/drivers.json"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.results;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.results(
# MAGIC   resultId INT,
# MAGIC   raceId INT,
# MAGIC   driverId INT,
# MAGIC   constructorId INT,
# MAGIC   number INT,
# MAGIC   grid INT,
# MAGIC   position INT,
# MAGIC   positionText STRING,
# MAGIC   positionOrder INT,
# MAGIC   points FLOAT,
# MAGIC   laps INT,
# MAGIC   time STRING,
# MAGIC   milliseconds INT,
# MAGIC   fastestLap INT,
# MAGIC   rank INT,
# MAGIC   fastestLapTime STRING,
# MAGIC   fastestLapSpeed FLOAT,
# MAGIC   statusId INT)
# MAGIC   USING JSON
# MAGIC   OPTIONS(path "/mnt/fourmla1dl/raw/results.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.results

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.pit_stops;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
# MAGIC   driverId INT,
# MAGIC   duration STRING,
# MAGIC   lap INT,
# MAGIC   milliseconds INT,
# MAGIC   raceId INT,
# MAGIC   stop INT,
# MAGIC   timestamp TIMESTAMP)
# MAGIC   USING JSON
# MAGIC   OPTIONS (PATH "/mnt/fourmla1dl/raw/pit_stops.json", multiline true)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.pit_stops

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.lap_times; 
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
# MAGIC   raceId INT,
# MAGIC   driverId INT,
# MAGIC   lap INT,
# MAGIC   position INT,
# MAGIC   time STRING,
# MAGIC   milliseconds INT
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS (PATH "/mnt/fourmla1dl/raw/lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM f1_raw.lap_times;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.qualifying;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
# MAGIC   qualifyId INT,
# MAGIC   raceId INT,
# MAGIC   driverId INT,
# MAGIC   constructorId INT,
# MAGIC   number INT,
# MAGIC   position INT,
# MAGIC   q1 STRING,
# MAGIC   q2 STRING,
# MAGIC   q3 STRING)
# MAGIC   USING JSON
# MAGIC   OPTIONS (PATH "/mnt/fourmla1dl/raw/qualifying", multiline true)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.qualifying

# COMMAND ----------

