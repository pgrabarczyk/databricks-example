# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Task_1_Build_sample_ETL

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Outcome:
# MAGIC 
# MAGIC **Build sample ETL**
# MAGIC 
# MAGIC It should help to show probability if your flight will be cancelled depend from origin.
# MAGIC 
# MAGIC (This task is just to show ETL, not have to make sence of business logic)
# MAGIC 
# MAGIC ![Databricks_sample-Task_1.png](https://github.com/pgrabarczyk/databricks-sample/raw/master/images/Task1/Databricks_sample-Task_1.png)
# MAGIC 
# MAGIC ### Subtasks:
# MAGIC 
# MAGIC * 1.1 Prepare S3 bucket with sample data
# MAGIC * 1.2 Mount S3, to use as dbfs
# MAGIC * 1.3 Create a bronze table with and batch load there data from sample S3 to Bronze table (in S3 dir there will be just raw CSV files)
# MAGIC * 1.4 Create a silver table. Data should be taken from Bronze table and transformed. Silver table data should use the Delta format
# MAGIC * 1.5 Create a gold table. Data should be taken from Silver table and transformed. Gold table should contain data to read by consumers

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 1.1 Prepare S3 bucket with sample data

# COMMAND ----------

### Create S3 bucket

```bash
export S3_BUCKET_NAME=pgrabarczyk-test-data
# Create bucket... I've done it manually...
```

### Download locally https://www.kaggle.com/divyansh22/flight-delay-prediction, then upload to S3 bucket
```bash
$ aws s3 ls $S3_BUCKET_NAME/January_Flight_Delay_Prediction/
2022-02-04 13:08:39   76028274 Jan_2019_ontime.csv
2022-02-04 13:08:39   79258578 Jan_2020_ontime.csv
```


### Create IAM Role and Cluster
// TODO https://docs.databricks.com/administration-guide/cloud-configurations/aws/instance-profiles.html
// I've created cluster and will use AWS credential for this experiment

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 1.2 Mount S3, to use as dbfs

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Keys for user pgrabarczyk-databricks which has access to S3 buckets only ... yes I know I should use IAM roles... it's just an sandbox env...
# MAGIC access_key = 'XXXX'
# MAGIC secret_key = 'XXXX'
# MAGIC 
# MAGIC encoded_secret_key = secret_key.replace("/", "%2F")
# MAGIC aws_bucket_name = "pgrabarczyk-test-data"
# MAGIC mount_name = "test_data"
# MAGIC 
# MAGIC # dbutils.fs.unmount("/mnt/%s" % mount_name)
# MAGIC dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name), "/mnt/%s" % mount_name)
# MAGIC display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Check if data has been mounted

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

# MAGIC %fs ls /mnt/test_data/January_Flight_Delay_Prediction/

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Test (Create table and execute sample query)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS DB_Task_1;
# MAGIC USE DB_Task_1;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS Flight_Delay_Prediction
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC  header="true",
# MAGIC  delimeter=",",
# MAGIC  inferSchema="true",
# MAGIC  path="dbfs:/mnt/test_data/January_Flight_Delay_Prediction/Jan_2019_ontime.csv"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM Flight_Delay_Prediction;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Sample data in S3 bucket looks like this:
# MAGIC 
# MAGIC ![s3_bucket_01.PNG](https://github.com/pgrabarczyk/databricks-sample/raw/master/images/Task1/s3_bucket_01.PNG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Create a bronze table with and batch load there data from sample S3 to Bronze table (in S3 dir there will be just raw CSV files)
# MAGIC 
# MAGIC In this example we already have a table from previous example... so I'll use it. In next examples I should try something more advanced.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC USE DB_Task_1;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS Bronze_Flight_Delay_Prediction;
# MAGIC 
# MAGIC CREATE TABLE Bronze_Flight_Delay_Prediction
# MAGIC USING CSV
# MAGIC LOCATION "/bronze/Flight_Delay_Prediction"
# MAGIC AS (
# MAGIC   SELECT *
# MAGIC   FROM Flight_Delay_Prediction
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### What's inside the bronze table?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM Bronze_Flight_Delay_Prediction;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The same as in the origin table. Because it should be original / raw data which can be reused in the future in case of changing business requirements.
# MAGIC We can go deeper and check columns of new table.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE Bronze_Flight_Delay_Prediction;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Or use **EXTENDED** version of describe to check metadata too.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED Bronze_Flight_Delay_Prediction;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### How Can I transform the data?
# MAGIC 
# MAGIC Let's find something interesting for sake of this example.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT CANCELLED, COUNT(*)
# MAGIC FROM Bronze_Flight_Delay_Prediction
# MAGIC GROUP BY CANCELLED;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT ORIGIN, COUNT(*)
# MAGIC FROM Bronze_Flight_Delay_Prediction
# MAGIC WHERE CANCELLED = 1
# MAGIC GROUP BY ORIGIN;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC How the Bronze table looks like on S3? There are just raw `csv` files.
# MAGIC 
# MAGIC ![s3_bucket_02_bronze.PNG](https://github.com/pgrabarczyk/databricks-sample/raw/master/images/Task1/s3_bucket_02_bronze.PNG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Create a silver table. Data should be taken from Bronze table and transformed. Silver table data should use the Delta format

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- DROP TABLE IF EXISTS Silver_Flight_Cancelled;
# MAGIC 
# MAGIC CREATE TABLE Silver_Flight_Cancelled                    -- We want to know if flight from Origin has been cancelled, that's why name changed
# MAGIC USING DELTA
# MAGIC LOCATION "/silver/Flight_Cancelled"
# MAGIC AS (
# MAGIC   SELECT ORIGIN, CANCELLED
# MAGIC   FROM Bronze_Flight_Delay_Prediction
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM Silver_Flight_Cancelled;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Our Silver_Flight_Cancelled contains parquet files in S3.
# MAGIC 
# MAGIC ![s3_bucket_03_silver.PNG](https://github.com/pgrabarczyk/databricks-sample/raw/master/images/Task1/s3_bucket_03_silver.PNG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 Create a gold table. Data should be taken from Silver table and transformed. Gold table should contain data to read by consumers

# COMMAND ----------

# MAGIC %md
# MAGIC What we want to achieve is fast count of cancelled and not cancelled flights for each origin. Check how long we would have to wait for single SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT ORIGIN,
# MAGIC     SUM(CASE WHEN CANCELLED = 0 THEN 1 ELSE 0 END) AS CANCELLED,
# MAGIC     SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) AS NOT_CANCELLED
# MAGIC FROM Silver_Flight_Cancelled
# MAGIC GROUP BY ORIGIN

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC In my case for 2 workers (AWS `m5.large` with 8 GB memory and 2 CPU Cores) it takes around 16 seconds. That's why want to transform this data once again to fast read by clients. 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS Gold_Flight_Cancelled;
# MAGIC 
# MAGIC CREATE TABLE Gold_Flight_Cancelled
# MAGIC USING DELTA
# MAGIC LOCATION "/gold/Flight_Cancelled"
# MAGIC AS (
# MAGIC   SELECT ORIGIN,
# MAGIC       SUM(CASE WHEN CANCELLED = 0 THEN 1 ELSE 0 END) AS CANCELLED,
# MAGIC       SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) AS NOT_CANCELLED
# MAGIC   FROM Silver_Flight_Cancelled
# MAGIC   GROUP BY ORIGIN
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Gold table is ready (Command took 1.54 minutes). Now we can exeute a query on it.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM Gold_Flight_Cancelled;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC In my case select on the gold table took 0.72 seconds.
# MAGIC 
# MAGIC So the speedup was from ~16 seconds to less than 1.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Go ahead and update the chart (click on `Plot Options` and set and use `ORIGIN` as Keys `CANCELLED` as Values).
# MAGIC 
# MAGIC ![cancelled_flights_charts.PNG](https://github.com/pgrabarczyk/databricks-sample/raw/master/images/Task1/cancelled_flights_charts.PNG)
