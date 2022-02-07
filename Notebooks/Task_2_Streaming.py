# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Task_2_Streaming
# MAGIC 
# MAGIC ## Outcome:
# MAGIC 
# MAGIC **Build ETL with streaming data (auto-update)**
# MAGIC 
# MAGIC TODO
# MAGIC 
# MAGIC ![Databricks_sample-Task_2.png](https://github.com/pgrabarczyk/databricks-sample/raw/master/images/Task2/Databricks_sample-Task_2.png)
# MAGIC 
# MAGIC ### Subtasks:
# MAGIC * 2.1 Preparation Goal - Simulate a stream
# MAGIC   * 2.1.1 Test Data
# MAGIC   * 2.1.2 Custom Script
# MAGIC   * 2.1.3 Temporary SQL View Data Source 
# MAGIC * 2.2 Main Goal
# MAGIC   * 2.2.1 Consume stream and persist it into Bronze table `bronze/Stack_Overflow_Surveys` 
# MAGIC   * 2.2.2 Stream data from Bronze to the Silver table `silver/Stack_Overflow_Surveys` + transform (clean the data)
# MAGIC   * 2.2.3 Stream live data to the Gold table `gold/???`
# MAGIC   * 2.2.4 Create Gold table `gold/???` and enable scheduled job to update data with newest

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 2.1 Preparation Goal - Simulate a stream
# MAGIC 
# MAGIC I will store datatest at `Test Data` S3 bucket, then using python script move file by file to another S3 bucket localization ( `result_bucket/.../task2` ) which will be used for SQL View `Stack_Overflow_Surveys`.
# MAGIC 
# MAGIC This SQL View will be used to stream data.
# MAGIC 
# MAGIC ### Dataset
# MAGIC 
# MAGIC Original dataset has been taken from: https://www.kaggle.com/klmsathishkumar/stack-overflow-dataset

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1.1 Test Data
# MAGIC 
# MAGIC I splitted ~200MB CSV file into 889 smaller for this example, using: https://github.com/pgrabarczyk/csv-file-splitter.
# MAGIC 
# MAGIC Then upload all parts to the `Test Data` S3 bucket.

# COMMAND ----------

# MAGIC %fs ls /mnt/test_data/survey_results_public_splitted/

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We'll copy files between two S3. `Test Data` is already mounted. We need to mount `Result Bucket` where silver, bronze and gold tables exists.
# MAGIC 
# MAGIC Let's copy & paste & use from Task_1 to mount it.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Keys for user which has access to S3 buckets only ... yes I know I should use IAM roles... it's just an sandbox env...
# MAGIC access_key = 'XXXX'
# MAGIC secret_key = 'XXXX'
# MAGIC 
# MAGIC encoded_secret_key = secret_key.replace("/", "%2F")
# MAGIC aws_bucket_name = "db-b76b6bc5e884270c34e82770cd5b9eb0-s3-root-bucket"
# MAGIC mount_name = "result_bucket"
# MAGIC 
# MAGIC # dbutils.fs.unmount("/mnt/%s" % mount_name)
# MAGIC dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name), "/mnt/%s" % mount_name)
# MAGIC display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 2.1.2 Custom Script
# MAGIC 
# MAGIC This script should copy file by file from one S3 dir to another (it will mock delivery of data).

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC source_dir        = "dbfs:/mnt/test_data/survey_results_public_splitted"
# MAGIC dest_dir          = "dbfs:/mnt/result_bucket/ireland-prod/1884956493483554/task_2"
# MAGIC checkpoints_dir   = f'{dest_dir}/checkpoints'
# MAGIC 
# MAGIC # table/views paths
# MAGIC view_path_source  = f'{dest_dir}/source/Stack_Overflow_Surveys'
# MAGIC table_path_bronze = f'{dest_dir}/bronze/Stack_Overflow_Surveys'
# MAGIC table_path_silver = f'{dest_dir}/silver/' #TODO
# MAGIC table_path_gold   = f'{dest_dir}/gold/'   #TODO
# MAGIC 
# MAGIC # checkpoints paths - to know which file was already processed
# MAGIC checkpoint_source = f'{checkpoints_dir}/source/Stack_Overflow_Surveys'
# MAGIC checkpoint_bronze = f'{checkpoints_dir}/bronze/Stack_Overflow_Surveys'
# MAGIC checkpoint_silver = f'{checkpoints_dir}/silver/' #TODO
# MAGIC checkpoint_gold   = f'{checkpoints_dir}/gold/'   #TODO
# MAGIC 
# MAGIC # Create directories
# MAGIC dbutils.fs.mkdirs(view_path_source)
# MAGIC dbutils.fs.mkdirs(table_path_bronze)
# MAGIC dbutils.fs.mkdirs(table_path_silver)
# MAGIC dbutils.fs.mkdirs(table_path_gold)
# MAGIC dbutils.fs.mkdirs(checkpoint_source)
# MAGIC dbutils.fs.mkdirs(checkpoint_bronze)
# MAGIC dbutils.fs.mkdirs(checkpoint_silver)
# MAGIC dbutils.fs.mkdirs(checkpoint_gold)
# MAGIC 
# MAGIC import shutil
# MAGIC 
# MAGIC class CopyFile():
# MAGIC     
# MAGIC     def __init__(self, source_dir: str, source_file_prefix: str, source_file_suffix: str, dest_dir: str, digits_in_suffix: int):
# MAGIC         self.source_dir : str = source_dir.replace('dbfs:', '/dbfs')
# MAGIC         self.source_file_prefix : str = source_file_prefix
# MAGIC         self.source_file_suffix : str = source_file_suffix
# MAGIC         self.dest_dir : str = dest_dir.replace('dbfs:', '/dbfs')
# MAGIC         self.actual_file_no : int = None
# MAGIC         self.digits_in_suffix : int = digits_in_suffix
# MAGIC         
# MAGIC     def _get_source_full_path(self) -> str:
# MAGIC         return f'{self.source_dir}/{self.source_file_prefix}{self.actual_file_no:0{self.digits_in_suffix}d}{self.source_file_suffix}'
# MAGIC     
# MAGIC     def _get_dest_full_path(self) -> str:
# MAGIC         return self._get_source_full_path().replace(self.source_dir, self.dest_dir)
# MAGIC         
# MAGIC     def _update_actual_file_no(self):
# MAGIC         if self.actual_file_no is None:
# MAGIC             self.actual_file_no = 1
# MAGIC         else:
# MAGIC             self.actual_file_no += 1
# MAGIC         
# MAGIC     def copy_next_file(self):
# MAGIC         self._update_actual_file_no()
# MAGIC         src_path = self._get_source_full_path()
# MAGIC         dest_path = self._get_dest_full_path()
# MAGIC         print(f'Copying {src_path} to the {dest_path}')
# MAGIC         shutil.copyfile(src_path, dest_path)
# MAGIC     
# MAGIC 
# MAGIC CopyFileInstance = CopyFile(
# MAGIC     source_dir = source_dir,
# MAGIC     source_file_prefix = 'survey_results_public_part_',
# MAGIC     source_file_suffix = '.csv',
# MAGIC     dest_dir = view_path_source,
# MAGIC     digits_in_suffix = 6
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Single execution of below should move one file / one part of dataset.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC CopyFileInstance.copy_next_file()
# MAGIC # dbutils.fs.ls(view_path_source)
# MAGIC display(dbutils.fs.ls(view_path_source))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 2.1.3 Temporary SQL View Data Source
# MAGIC 
# MAGIC Now we need a SQL View and Stream, to start sending the data.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC (spark.readStream
# MAGIC   .format('text')
# MAGIC   .schema('data STRING')
# MAGIC   .load(view_path_source) #  f'{dest_dir}/source/Stack_Overflow_Surveys'
# MAGIC   .createOrReplaceTempView('Source_Stack_Overflow_Surveys'))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Ok, what is happening so far?
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC We have a stream which continously `sending data`.
# MAGIC 
# MAGIC Producer is a SQL View `Source_Stack_Overflow_Surveys`, which reads files: `dbfs:/mnt/result_bucket/ireland-prod/1884956493483554/task_2/source/Stack_Overflow_Surveys/survey_results_public_part_******.csv`. Everytime new file occurs, then it is showed in SQL View.
# MAGIC 
# MAGIC Try to execute below SQL SELECT command and in the meantime execute (few times) command which will add new file:
# MAGIC 
# MAGIC ```python
# MAGIC %python
# MAGIC CopyFileInstance.copy_next_file()
# MAGIC ```
# MAGIC 
# MAGIC ![stream_producer.PNG](https://github.com/pgrabarczyk/databricks-sample/raw/master/images/Task2/stream_producer.PNG)
# MAGIC 
# MAGIC Data inside stream getting bigger. We're simulating the real stream and now we're ready to start the Main Goal.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) FROM Source_Stack_Overflow_Surveys;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 2.2 Main Goal
# MAGIC 
# MAGIC Our main goal is to consume the initial stream, then stream and transform data through all Delta Lake layers.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 2.2.1 Consume stream and persist it into Bronze table `bronze/Stack_Overflow_Surveys`

# COMMAND ----------


