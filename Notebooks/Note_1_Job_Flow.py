# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Note_1_Job_Flow
# MAGIC 
# MAGIC 
# MAGIC * Jobs can be scheduled or triggered manually or via API https://docs.databricks.com/dev-tools/api/latest/jobs.html
# MAGIC * Job is an agregate of Tasks
# MAGIC * Tasks can be executed in sequences or in parallel
# MAGIC * Task may depends on 1 or multiple tasks, if so then it will be executed after dependends task(s)
# MAGIC * Databricks.com draw DAG (Directed Acyclic Graph)
# MAGIC 
# MAGIC ![Job_Flow.PNG](https://github.com/pgrabarczyk/databricks-example/raw/master/images/Note1/Job_Flow.PNG)
