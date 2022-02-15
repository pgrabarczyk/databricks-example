# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Task_3_Insert_Update_Data
# MAGIC 
# MAGIC ## Outcome:
# MAGIC 
# MAGIC **Insert new data and update existing data**
# MAGIC 
# MAGIC ![Databricks_sample-Task_3.png](https://github.com/pgrabarczyk/databricks-sample/raw/master/images/Task3/Databricks_sample-Task_3.png)
# MAGIC 
# MAGIC ### Subtasks:
# MAGIC * 3.1 Preparation Goal
# MAGIC   * 3.1.1 Load CSV files into S3
# MAGIC   * 3.1.2 Create Bronze Table using file with prefix `_Part_00001`
# MAGIC   * 3.1.3 Use SQL copy to create and fill Silver Table
# MAGIC * 3.2 Main Goal
# MAGIC   * 3.2.1 Create Bronze Table using file with prefix `_Part_00002` and insert new data to the Silver Table
# MAGIC   * 3.2.2 Create Bronze Table using file with prefix `_Part_00003` and insert new data and update actual data to the Silver Table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 3.1 Preparation Goal

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 3.1.1 Load CSV files into S3
# MAGIC 
# MAGIC Files can be found here:
# MAGIC * [part_000001](https://github.com/pgrabarczyk/databricks-sample/raw/master/data/Task3/survey_results_public_part_000001.csv)
# MAGIC * [part_000002](https://github.com/pgrabarczyk/databricks-sample/raw/master/data/Task3/survey_results_public_part_000002.csv)
# MAGIC * [part_000003](https://github.com/pgrabarczyk/databricks-sample/raw/master/data/Task3/survey_results_public_part_000003.csv)
# MAGIC 
# MAGIC Load them to the S3.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC table_path_part_000001 = 'dbfs:/mnt/result_bucket/ireland-prod/1884956493483554/task_3/bronze/survey_results_public_part_000001.csv'
# MAGIC table_path_part_000002 = 'dbfs:/mnt/result_bucket/ireland-prod/1884956493483554/task_3/bronze/survey_results_public_part_000002.csv'
# MAGIC table_path_part_000003 = 'dbfs:/mnt/result_bucket/ireland-prod/1884956493483554/task_3/bronze/survey_results_public_part_000003.csv'
# MAGIC table_path_part_000003 = 'dbfs:/mnt/result_bucket/ireland-prod/1884956493483554/task_3/bronze/survey_results_public_part_000003.csv'
# MAGIC table_path_silver      = 'dbfs:/mnt/result_bucket/ireland-prod/1884956493483554/task_3/silver/Stack_Overflow_Surveys'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 3.1.2 Create Bronze Table using file with prefix `_Part_00001`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS Bronze_Stack_Overflow_Surveys_part_000001;
# MAGIC DROP TABLE IF EXISTS Bronze_Stack_Overflow_Surveys_part_000002;
# MAGIC DROP TABLE IF EXISTS Bronze_Stack_Overflow_Surveys_part_000003;
# MAGIC DROP TABLE IF EXISTS Silver_Stack_Overflow_Surveys;
# MAGIC 
# MAGIC DROP DATABASE DB_Task_3;
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS DB_Task_3;
# MAGIC USE DB_Task_3;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE Bronze_Stack_Overflow_Surveys_part_000001
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC  header="true",
# MAGIC  delimeter=",",
# MAGIC  inferSchema="true",
# MAGIC  path="dbfs:/mnt/result_bucket/ireland-prod/1884956493483554/task_3/bronze/survey_results_public_part_000001.csv"
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM Bronze_Stack_Overflow_Surveys_part_000001;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 3.1.3 Use SQL copy to create and fill Silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE Silver_Stack_Overflow_Surveys
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/mnt/result_bucket/ireland-prod/1884956493483554/task_3/silver/Stack_Overflow_Surveys'
# MAGIC AS (
# MAGIC   SELECT *
# MAGIC   FROM Bronze_Stack_Overflow_Surveys_part_000001
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) FROM Silver_Stack_Overflow_Surveys;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM Silver_Stack_Overflow_Surveys
# MAGIC WHERE Respondent = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 3.2 Main Goal

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 3.2.1 Create Bronze Table using file with prefix `_Part_00002` and insert new data to the Silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE Bronze_Stack_Overflow_Surveys_part_000002
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC  header="true",
# MAGIC  delimeter=",",
# MAGIC  inferSchema="true",
# MAGIC  path="dbfs:/mnt/result_bucket/ireland-prod/1884956493483554/task_3/bronze/survey_results_public_part_000002.csv"
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM Bronze_Stack_Overflow_Surveys_part_000002;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Ok, we have a table of missing data need to be inserted to the Silver table. Let's use `MERGE` command to do it.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO Silver_Stack_Overflow_Surveys SILVER
# MAGIC    USING Bronze_Stack_Overflow_Surveys_part_000002 BRONZE_000002
# MAGIC    
# MAGIC    ON SILVER.Respondent = BRONZE_000002.Respondent
# MAGIC    
# MAGIC    WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Previously there were 100 rows inside Silver table. Now there should be 100 more, then 200.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) FROM Silver_Stack_Overflow_Surveys;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 3.2.2 Create Bronze Table using file with prefix `_Part_00003` and insert new data and update actual data to the Silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE Bronze_Stack_Overflow_Surveys_part_000003
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC  header="true",
# MAGIC  delimeter=",",
# MAGIC  inferSchema="true",
# MAGIC  path="dbfs:/mnt/test_data/Task3/survey_results_public_part_000003.csv"
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM Bronze_Stack_Overflow_Surveys_part_000003;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC I've prepared `part_000003` to have some rows with the same `Respondent` as already existing in Silver Table.
# MAGIC 
# MAGIC I want to update rows where `Respondent` is the same, and insert rows where `Respondent` not exists. 
# MAGIC 
# MAGIC Let's find how much `Respondent` are already exists in `part_000003` and in `Silver Table`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*)
# MAGIC FROM Silver_Stack_Overflow_Surveys SILVER, Bronze_Stack_Overflow_Surveys_part_000003 BRONZE_000003
# MAGIC WHERE SILVER.Respondent = BRONZE_000003.Respondent;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Ok, so 10 rows should be updated (with Respondent from 1 to 10) and 90 should be inserted (there should be 290 total).
# MAGIC 
# MAGIC I'll update just few columns (I could use `Update SET *`, but I want show how to update exact columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO Silver_Stack_Overflow_Surveys SILVER
# MAGIC    USING Bronze_Stack_Overflow_Surveys_part_000003 BRONZE_000003
# MAGIC    
# MAGIC    ON SILVER.Respondent = BRONZE_000003.Respondent
# MAGIC    
# MAGIC    WHEN MATCHED THEN
# MAGIC --    DELETE
# MAGIC --    UPDATE SET *
# MAGIC      UPDATE SET
# MAGIC        SILVER.MainBranch = BRONZE_000003.MainBranch,
# MAGIC        SILVER.Hobbyist = BRONZE_000003.Hobbyist,
# MAGIC        SILVER.OpenSourcer = BRONZE_000003.OpenSourcer
# MAGIC    
# MAGIC    WHEN NOT MATCHED THEN
# MAGIC      INSERT *
# MAGIC --      INSERT (column1 [, ...] ) VALUES (value1 [, ...])

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) FROM Silver_Stack_Overflow_Surveys;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM Silver_Stack_Overflow_Surveys;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC I can see that first 10 rows changed and there is 290 rows. Well done.
