# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Task Extra 1
# MAGIC 
# MAGIC 
# MAGIC Dataset used for this task: https://www.kaggle.com/jessevent/all-crypto-currencies/download
# MAGIC 
# MAGIC ## Subtasks:
# MAGIC * 1.1 Create two tables - one with partitioning and one without, try to compare query times

# COMMAND ----------

# MAGIC %fs ls /mnt/test_data/crypto-markets.csv

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 1.1 Create two tables - one with partitioning and one without, try to compare query times

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Cleanup
# MAGIC 
# MAGIC DROP VIEW IF EXISTS Crypto_Markets;
# MAGIC DROP VIEW IF EXISTS Crypto_Markets_Top;
# MAGIC DROP TABLE IF EXISTS Crypto_Markets_Without_Partitioning;
# MAGIC DROP TABLE IF EXISTS Crypto_Markets_With_Partitioning;
# MAGIC DROP DATABASE IF EXISTS DB_Task_E1;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS DB_Task_E1;
# MAGIC USE DB_Task_E1;
# MAGIC 
# MAGIC CREATE TEMPORARY VIEW Crypto_Markets
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC  header="true",
# MAGIC  delimeter=",",
# MAGIC  inferSchema="true",
# MAGIC  path="dbfs:/mnt/test_data/crypto-markets.csv"
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC There are too much data for this example. I'll limit it to 30 curriences.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TEMPORARY VIEW Crypto_Markets_Top
# MAGIC AS (
# MAGIC   SELECT symbol
# MAGIC   FROM Crypto_Markets
# MAGIC   GROUP BY symbol
# MAGIC   ORDER BY COUNT(*) DESC
# MAGIC   LIMIT 30
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE Crypto_Markets_Without_Partitioning
# MAGIC USING DELTA
# MAGIC LOCATION "/e1/Crypto_Markets_Without_Partitioning"
# MAGIC AS (
# MAGIC   SELECT *
# MAGIC   FROM Crypto_Markets
# MAGIC   WHERE symbol IN ( 
# MAGIC     SELECT symbol
# MAGIC     FROM Crypto_Markets_Top
# MAGIC   )
# MAGIC );

# COMMAND ----------

#md

Command took 10.15 seconds

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE Crypto_Markets_With_Partitioning
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (symbol)
# MAGIC LOCATION "/e1/Crypto_Markets_With_Partitioning"
# MAGIC AS (
# MAGIC   SELECT *
# MAGIC   FROM Crypto_Markets
# MAGIC   WHERE symbol IN ( 
# MAGIC     SELECT symbol
# MAGIC     FROM Crypto_Markets_Top
# MAGIC   )
# MAGIC );

# COMMAND ----------

#md

Command took 11.47 seconds

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Difference with files
# MAGIC 
# MAGIC It's worth to take a look on S3 bucket files. In case of Crypto_Markets_With_Partitioning, there are multiple folders for each partition.
# MAGIC 
# MAGIC 
# MAGIC | Table name  | Folders |
# MAGIC |---|---|---|
# MAGIC | `Crypto_Markets_Without_Partitioning` | ![Crypto_Markets_Without_Partitioning.PNG](https://github.com/pgrabarczyk/databricks-sample/raw/master/images/TaskE1/Crypto_Markets_Without_Partitioning.PNG) |
# MAGIC | `Crypto_Markets_With_Partitioning` | ![Crypto_Markets_With_Partitioning.PNG](https://github.com/pgrabarczyk/databricks-sample/raw/master/images/TaskE1/Crypto_Markets_With_Partitioning.PNG) |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Performance tests
# MAGIC 
# MAGIC Let's execute two queries type. First, filtering by one symbol, Second without filtering.
# MAGIC 
# MAGIC It worth to mention about actual tables size and number of files: 
# MAGIC 
# MAGIC | Table name  | Size (MB) | Number of files |
# MAGIC |---|---|---|
# MAGIC | `Crypto_Markets_With_Partitioning` | 3.03 MB | 34 |
# MAGIC | `Crypto_Markets_Without_Partitioning` | 2.71 MB | 2 |
# MAGIC 
# MAGIC 
# MAGIC **NOTE**
# MAGIC I'll not execute a professional tests. I'll just execute them few times and check if I can observe something using my special environment. Don't use my statistics to blame any creator of technology which is used here :). 

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // As I'll execute the same query several times I'll temporary turn off cache to compare results.
# MAGIC 
# MAGIC spark.conf.set("spark.databricks.io.cache.enabled", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Test 1 - query on 1 symbol

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CLEAR CACHE;
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM Crypto_Markets_Without_Partitioning
# MAGIC WHERE symbol = 'BTC';

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CLEAR CACHE;
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM Crypto_Markets_With_Partitioning
# MAGIC WHERE symbol = 'BTC';

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Some statistics about my tests:
# MAGIC 
# MAGIC | Table name (query executed on) | Execute 1 | Execute 2 | Execute 3 |  Execute 4 | Execute 5 |
# MAGIC |---|---|---|---|---|---|
# MAGIC | `Crypto_Markets_Without_Partitioning` | 0.96s | 1.14s | 0.75s | 0.81s | 1.06s |
# MAGIC | `Crypto_Markets_With_Partitioning` | 0.48s | 0.93s | 0.47s | 0.53s  | 0.66s |
# MAGIC 
# MAGIC On ~3MB database and filtered (using 1 parameter) query can takes 2x more time without partitioning.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Test 2 - query on all symbols

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CLEAR CACHE;
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM Crypto_Markets_Without_Partitioning;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CLEAR CACHE;
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM Crypto_Markets_With_Partitioning;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Some statistics about my tests:
# MAGIC 
# MAGIC | Table name (query executed on) | Execute 1 | Execute 2 | Execute 3 |  Execute 4 | Execute 5 |
# MAGIC |---|---|---|---|---|---|
# MAGIC | `Crypto_Markets_Without_Partitioning` | 1.23s | 0.49s | 0.48s | 0.55s | 0.44s |
# MAGIC | `Crypto_Markets_With_Partitioning` | 0.75s | 0.51s | 0.48s | 0.49s  | 0.46s |
# MAGIC 
# MAGIC There is no observation for such little data & tests execution.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // Enable cache back
# MAGIC 
# MAGIC spark.conf.set("spark.databricks.io.cache.enabled", "true")
