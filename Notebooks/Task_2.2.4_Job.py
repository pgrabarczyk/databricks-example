# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### 2.2.4 Create Gold table gold/Earnings and enable scheduled job to update data with newest
# MAGIC 
# MAGIC Table gold `Gold_Earnings` no need to live updated like `Gold_Satisfaction`.
# MAGIC 
# MAGIC First, we need to import some data from previous notebook...

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC source_dir        = "dbfs:/mnt/test_data/survey_results_public_splitted"
# MAGIC dest_dir          = "dbfs:/mnt/result_bucket/ireland-prod/1884956493483554/task_2"
# MAGIC checkpoints_dir   = f'{dest_dir}/checkpoints'
# MAGIC 
# MAGIC # table/views paths
# MAGIC table_path_silver            = f'{dest_dir}/silver/Stack_Overflow_Surveys'
# MAGIC table_path_gold_earnings     = f'{dest_dir}/gold/Earnings'
# MAGIC 
# MAGIC # checkpoints paths - to know which file was already processed
# MAGIC checkpoint_gold_earnings     = f'{checkpoints_dir}/gold/Earnings'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ... then create a view which will keep transformed data...

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC (spark.readStream
# MAGIC   .format('delta')
# MAGIC   .load(table_path_silver)
# MAGIC   .createOrReplaceTempView('Silver_Stack_Overflow_Surveys'))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Gold_Earnings_Supply
# MAGIC AS (
# MAGIC   SELECT
# MAGIC     silver_stack_overflow_surveys.YearsCodePro,
# MAGIC     silver_stack_overflow_surveys.ConvertedComp
# MAGIC   FROM Silver_Stack_Overflow_Surveys
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- SELECT COUNT(*) FROM Gold_Earnings_Supply;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ... then use one time stream to delivery actual data, but executed only **once** from time to time. TO do that we need to use `.trigger(once=True)`

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC (spark.table('Gold_Earnings_Supply')
# MAGIC   .writeStream
# MAGIC   .format('delta')
# MAGIC   .outputMode('append')
# MAGIC   .option('checkpointLocation', checkpoint_gold_earnings)
# MAGIC   .trigger(once=True)
# MAGIC   .start(table_path_gold_earnings))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You can now confirm new files has been created in gold folder in S3 bucket.
# MAGIC 
# MAGIC I'll create a temporary view here and check what is inside.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC (spark.readStream
# MAGIC   .format('delta')
# MAGIC   .load(table_path_gold_earnings)
# MAGIC   .createOrReplaceTempView('Gold_Earnings'))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- SELECT COUNT(*) FROM Gold_Earnings;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Create a Job
# MAGIC 
# MAGIC I've created a job with new dedicated job cluster.
# MAGIC 
# MAGIC 
# MAGIC ![Create_Job_2.PNG](https://github.com/pgrabarczyk/databricks-example/raw/master/images/Task2/Create_Job_2.PNG)
# MAGIC ![Job_for_Task_2.2.4.PNG](https://github.com/pgrabarczyk/databricks-example/raw/master/images/Task2/Job_for_Task_2.2.4.PNG)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC First time, my task had problem to finish, (probably) because above there was a uncommented SQL select which is using streaming. I had to comment this line, then it worked.
# MAGIC 
# MAGIC For tests purpose you can execute `CopyFileInstance.copy_next_file()` from previous Notebook and execute job again. New files should occured and new data in temporary view.
