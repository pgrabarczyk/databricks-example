# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Task_4_Change_Data_Capture
# MAGIC 
# MAGIC ## Outcome:
# MAGIC 
# MAGIC **Stream 'changes' in DB through Databricks, transform it, then stream it to the Amazon Redshift**
# MAGIC 
# MAGIC ![Databricks_sample-Task_4.png](https://github.com/pgrabarczyk/databricks-sample/raw/master/images/Task4/Databricks_sample-Task_4.png)
# MAGIC 
# MAGIC ### Subtasks:
# MAGIC * 4.1 Preparation Goal
# MAGIC   * 4.1.1 AWS IAM Role Setup for Cluster
# MAGIC   * 4.1.2 Create AWS EC2 and setup PostgreSQL, Debezium, Apache Zookeeper, Apache Kafka
# MAGIC   * 4.1.3 Create Amazon Redshift
# MAGIC * 4.2 Main Goal
# MAGIC   * 4.2.1 Create Bronze Table to gather data from Apache Kafka as it is
# MAGIC   * 4.2.2 Stream and transform data from Bronze Table to Silver Table. Silver table should contain parsed data
# MAGIC   * 4.2.3 Stream and transform data from Silver Table to Gold Table. Gold table should contain data ready to consume by Amazon Redshift.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 4.1 Preparation Goal

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Keys for user which has access to S3 buckets only ... yes I know I should use IAM roles... it's just an sandbox env...
# MAGIC access_key = 'X'
# MAGIC secret_key = 'XXXXX'
# MAGIC 
# MAGIC encoded_secret_key = secret_key.replace("/", "%2F")
# MAGIC aws_bucket_name = "db-0e4f35b03d1d03c07c73eba18118850c-s3-root-bucket"
# MAGIC mount_name = "result_bucket"
# MAGIC 
# MAGIC # dbutils.fs.unmount("/mnt/%s" % mount_name)
# MAGIC dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name), "/mnt/%s" % mount_name)
# MAGIC display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 4.1.1 AWS IAM Role Setup for Cluster
# MAGIC 
# MAGIC As my tasks are more like Proof of Concepts I didn't had too much time to property configure Databricks to automatic setup cluster with IAM Role.
# MAGIC 
# MAGIC If you're interested in, you should probably check [this](https://docs.databricks.com/administration-guide/cloud-configurations/aws/instance-profiles.html)
# MAGIC 
# MAGIC For this scenario (PoC tasks + Sandbox environment), I'll run cluster `Runtime: 9.1 LTS (Scala 2.12, Spark 3.1.2)` and manually add IAM Role with full access..
# MAGIC 
# MAGIC So... I created EC2: `t3.medium`, in `databricks VPC`, `private subnet`, OS: `Ubuntu 20.04`, `30GB volume`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 4.1.2 Create AWS EC2 and setup PostgreSQL, Debezium, Apache Zookeeper, Apache Kafka
# MAGIC 
# MAGIC I'll use [Debezium documentation](https://debezium.io/documentation/reference/stable/tutorial.html) and [this blog post](https://www.startdataengineering.com/post/change-data-capture-using-debezium-kafka-and-pg/) to setup environment.
# MAGIC 
# MAGIC I'm using AWS Session Manager to jump on EC2.
# MAGIC 
# MAGIC then execute:
# MAGIC 
# MAGIC ```bash
# MAGIC bash
# MAGIC cd $(mktemp -d)
# MAGIC ```
# MAGIC 
# MAGIC You can save below script as file and execute it (e.g.: ```bash docker.sh```) or execute it line by line in your terminal.

# COMMAND ----------

#!/usr/bin/env bash
# docker.sh
set -eu

# Install Docker https://docs.docker.com/engine/install/ubuntu/
sudo apt-get update
sudo apt-get install \
    ca-certificates \
    curl \
    gnupg \
    lsb-release
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg    
echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io

# Docker Test1
sudo docker run hello-world

# Use docker without sudo
sudo groupadd docker || true
sudo usermod -aG docker $(whoami)
newgrp docker
bash

# Docker Test1
docker run hello-world

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now setup IP address of your EC2 instance, in my case it's '10.225.181.154' and export it.
# MAGIC 
# MAGIC ```bash
# MAGIC export ADVERTISED_HOST_NAME=10.142.225.168
# MAGIC ```
# MAGIC 
# MAGIC Copy below one by line (specially part of setting DB rows).

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC # Setup Postgres
# MAGIC docker run -d --name postgres -p 5432:5432 -e POSTGRES_USER=start_data_engineer -e POSTGRES_PASSWORD=password debezium/postgres:12
# MAGIC 
# MAGIC # Setup zookeeper & kafka
# MAGIC docker run -d --name zookeeper -p 2181:2181 -p 2888:2888 -p 3888:3888 debezium/zookeeper:1.1
# MAGIC docker run -d --name kafka -p 9092:9092 -e ADVERTISED_HOST_NAME --link zookeeper:zookeeper debezium/kafka:1.1 
# MAGIC 
# MAGIC ## Add data to Postgres
# MAGIC sudo apt-get install -y pgcli jq   
# MAGIC 
# MAGIC PGPASSWORD=password pgcli -h localhost -p 5432 -U start_data_engineer
# MAGIC 
# MAGIC CREATE SCHEMA bank;
# MAGIC SET search_path TO bank,public;
# MAGIC CREATE TABLE bank.holding (
# MAGIC     holding_id int,
# MAGIC     user_id int,
# MAGIC     holding_stock varchar(8),
# MAGIC     holding_quantity int,
# MAGIC     datetime_created timestamp,
# MAGIC     datetime_updated timestamp,
# MAGIC     primary key(holding_id)
# MAGIC );
# MAGIC ALTER TABLE bank.holding replica identity FULL;
# MAGIC insert into bank.holding values (1000, 1, 'VFIAX', 10, now(), now());
# MAGIC \q
# MAGIC # then 'y'
# MAGIC 
# MAGIC docker run -d --name connect -p 8083:8083 --link kafka:kafka \
# MAGIC --link postgres:postgres -e BOOTSTRAP_SERVERS=kafka:9092 \
# MAGIC -e GROUP_ID=sde_group -e CONFIG_STORAGE_TOPIC=sde_storage_topic \
# MAGIC -e OFFSET_STORAGE_TOPIC=sde_offset_topic debezium/connect:1.1
# MAGIC 
# MAGIC sleep 10 # or you can just wait...
# MAGIC 
# MAGIC curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{"name": "sde-connector", "config": {"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "database.hostname": "postgres", "database.port": "5432", "database.user": "start_data_engineer", "database.password": "password", "database.dbname" : "start_data_engineer", "database.server.name": "bankserver1", "table.whitelist": "bank.holding"}}'
# MAGIC 
# MAGIC # curl -H "Accept:application/json" localhost:8083/connectors/
# MAGIC 
# MAGIC # Test if data has been added
# MAGIC 
# MAGIC docker run -it --rm --name consumer --link zookeeper:zookeeper --link kafka:kafka debezium/kafka:1.1 watch-topic -a bankserver1.bank.holding --max-messages 1 | grep '^{' | jq

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC In my case I've JSON data with information that 1 row has been added (I deduced it from `before` and `after` parts).

# COMMAND ----------

# MAGIC %json
# MAGIC 
# MAGIC {
# MAGIC   "schema": {
# MAGIC     "type": "struct",
# MAGIC     "fields": [
# MAGIC       {
# MAGIC         "type": "struct",
# MAGIC         "fields": [
# MAGIC           {
# MAGIC             "type": "int32",
# MAGIC             "optional": false,
# MAGIC             "field": "holding_id"
# MAGIC           },
# MAGIC           {
# MAGIC             "type": "int32",
# MAGIC             "optional": true,
# MAGIC             "field": "user_id"
# MAGIC           },
# MAGIC           {
# MAGIC             "type": "string",
# MAGIC             "optional": true,
# MAGIC             "field": "holding_stock"
# MAGIC           },
# MAGIC           {
# MAGIC             "type": "int32",
# MAGIC             "optional": true,
# MAGIC             "field": "holding_quantity"
# MAGIC           },
# MAGIC           {
# MAGIC             "type": "int64",
# MAGIC             "optional": true,
# MAGIC             "name": "io.debezium.time.MicroTimestamp",
# MAGIC             "version": 1,
# MAGIC             "field": "datetime_created"
# MAGIC           },
# MAGIC           {
# MAGIC             "type": "int64",
# MAGIC             "optional": true,
# MAGIC             "name": "io.debezium.time.MicroTimestamp",
# MAGIC             "version": 1,
# MAGIC             "field": "datetime_updated"
# MAGIC           }
# MAGIC         ],
# MAGIC         "optional": true,
# MAGIC         "name": "bankserver1.bank.holding.Value",
# MAGIC         "field": "before"
# MAGIC       },
# MAGIC       {
# MAGIC         "type": "struct",
# MAGIC         "fields": [
# MAGIC           {
# MAGIC             "type": "int32",
# MAGIC             "optional": false,
# MAGIC             "field": "holding_id"
# MAGIC           },
# MAGIC           {
# MAGIC             "type": "int32",
# MAGIC             "optional": true,
# MAGIC             "field": "user_id"
# MAGIC           },
# MAGIC           {
# MAGIC             "type": "string",
# MAGIC             "optional": true,
# MAGIC             "field": "holding_stock"
# MAGIC           },
# MAGIC           {
# MAGIC             "type": "int32",
# MAGIC             "optional": true,
# MAGIC             "field": "holding_quantity"
# MAGIC           },
# MAGIC           {
# MAGIC             "type": "int64",
# MAGIC             "optional": true,
# MAGIC             "name": "io.debezium.time.MicroTimestamp",
# MAGIC             "version": 1,
# MAGIC             "field": "datetime_created"
# MAGIC           },
# MAGIC           {
# MAGIC             "type": "int64",
# MAGIC             "optional": true,
# MAGIC             "name": "io.debezium.time.MicroTimestamp",
# MAGIC             "version": 1,
# MAGIC             "field": "datetime_updated"
# MAGIC           }
# MAGIC         ],
# MAGIC         "optional": true,
# MAGIC         "name": "bankserver1.bank.holding.Value",
# MAGIC         "field": "after"
# MAGIC       },
# MAGIC       {
# MAGIC         "type": "struct",
# MAGIC         "fields": [
# MAGIC           {
# MAGIC             "type": "string",
# MAGIC             "optional": false,
# MAGIC             "field": "version"
# MAGIC           },
# MAGIC           {
# MAGIC             "type": "string",
# MAGIC             "optional": false,
# MAGIC             "field": "connector"
# MAGIC           },
# MAGIC           {
# MAGIC             "type": "string",
# MAGIC             "optional": false,
# MAGIC             "field": "name"
# MAGIC           },
# MAGIC           {
# MAGIC             "type": "int64",
# MAGIC             "optional": false,
# MAGIC             "field": "ts_ms"
# MAGIC           },
# MAGIC           {
# MAGIC             "type": "string",
# MAGIC             "optional": true,
# MAGIC             "name": "io.debezium.data.Enum",
# MAGIC             "version": 1,
# MAGIC             "parameters": {
# MAGIC               "allowed": "true,last,false"
# MAGIC             },
# MAGIC             "default": "false",
# MAGIC             "field": "snapshot"
# MAGIC           },
# MAGIC           {
# MAGIC             "type": "string",
# MAGIC             "optional": false,
# MAGIC             "field": "db"
# MAGIC           },
# MAGIC           {
# MAGIC             "type": "string",
# MAGIC             "optional": false,
# MAGIC             "field": "schema"
# MAGIC           },
# MAGIC           {
# MAGIC             "type": "string",
# MAGIC             "optional": false,
# MAGIC             "field": "table"
# MAGIC           },
# MAGIC           {
# MAGIC             "type": "int64",
# MAGIC             "optional": true,
# MAGIC             "field": "txId"
# MAGIC           },
# MAGIC           {
# MAGIC             "type": "int64",
# MAGIC             "optional": true,
# MAGIC             "field": "lsn"
# MAGIC           },
# MAGIC           {
# MAGIC             "type": "int64",
# MAGIC             "optional": true,
# MAGIC             "field": "xmin"
# MAGIC           }
# MAGIC         ],
# MAGIC         "optional": false,
# MAGIC         "name": "io.debezium.connector.postgresql.Source",
# MAGIC         "field": "source"
# MAGIC       },
# MAGIC       {
# MAGIC         "type": "string",
# MAGIC         "optional": false,
# MAGIC         "field": "op"
# MAGIC       },
# MAGIC       {
# MAGIC         "type": "int64",
# MAGIC         "optional": true,
# MAGIC         "field": "ts_ms"
# MAGIC       },
# MAGIC       {
# MAGIC         "type": "struct",
# MAGIC         "fields": [
# MAGIC           {
# MAGIC             "type": "string",
# MAGIC             "optional": false,
# MAGIC             "field": "id"
# MAGIC           },
# MAGIC           {
# MAGIC             "type": "int64",
# MAGIC             "optional": false,
# MAGIC             "field": "total_order"
# MAGIC           },
# MAGIC           {
# MAGIC             "type": "int64",
# MAGIC             "optional": false,
# MAGIC             "field": "data_collection_order"
# MAGIC           }
# MAGIC         ],
# MAGIC         "optional": true,
# MAGIC         "field": "transaction"
# MAGIC       }
# MAGIC     ],
# MAGIC     "optional": false,
# MAGIC     "name": "bankserver1.bank.holding.Envelope"
# MAGIC   },
# MAGIC   "payload": {
# MAGIC     "before": null,
# MAGIC     "after": {
# MAGIC       "holding_id": 1000,
# MAGIC       "user_id": 1,
# MAGIC       "holding_stock": "VFIAX",
# MAGIC       "holding_quantity": 10,
# MAGIC       "datetime_created": 1645105576905445,
# MAGIC       "datetime_updated": 1645105576905445
# MAGIC     },
# MAGIC     "source": {
# MAGIC       "version": "1.1.2.Final",
# MAGIC       "connector": "postgresql",
# MAGIC       "name": "bankserver1",
# MAGIC       "ts_ms": 1645105601680,
# MAGIC       "snapshot": "last",
# MAGIC       "db": "start_data_engineer",
# MAGIC       "schema": "bank",
# MAGIC       "table": "holding",
# MAGIC       "txId": 492,
# MAGIC       "lsn": 24605264,
# MAGIC       "xmin": null
# MAGIC     },
# MAGIC     "op": "r",
# MAGIC     "ts_ms": 1645105601684,
# MAGIC     "transaction": null
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### TODO 4.1.3 Create Amazon Redshift
# MAGIC 
# MAGIC TODO

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 4.2 Main Goal
# MAGIC 
# MAGIC Now we will consume stream from Kafka, and Transform it through Bronze, Silver and Gold Tables.
# MAGIC Before that, I'll prepare directories in AWS S3 for our [Delta Lake](https://databricks.com/product/delta-lake-on-databricks) and fill data about our existing EC2 and services.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC dest_dir          = "dbfs:/mnt/result_bucket/ireland-prod/1911096808398203/task_4"
# MAGIC checkpoints_dir   = f'{dest_dir}/checkpoints'
# MAGIC 
# MAGIC # tables/views paths
# MAGIC table_path_bronze            = f'{dest_dir}/bronze/Bank_Holding'
# MAGIC table_path_silver            = f'{dest_dir}/silver/Bank_Holding'
# MAGIC table_path_gold              = f'{dest_dir}/gold/Bank_Holding'
# MAGIC 
# MAGIC # checkpoints paths - to know which file was already processed
# MAGIC checkpoint_bronze            = f'{checkpoints_dir}/bronze/Bank_Holding'
# MAGIC checkpoint_silver            = f'{checkpoints_dir}/silver/Bank_Holding'
# MAGIC checkpoint_gold              = f'{checkpoints_dir}/gold/Bank_Holding'
# MAGIC 
# MAGIC # Create directories
# MAGIC dbutils.fs.mkdirs(table_path_bronze)
# MAGIC dbutils.fs.mkdirs(table_path_silver)
# MAGIC dbutils.fs.mkdirs(table_path_gold)
# MAGIC dbutils.fs.mkdirs(checkpoint_bronze)
# MAGIC dbutils.fs.mkdirs(checkpoint_silver)
# MAGIC dbutils.fs.mkdirs(checkpoint_gold)
# MAGIC 
# MAGIC # Infrastructure details
# MAGIC EC2_IP      = '10.142.225.168'
# MAGIC KAFKA_PORT  = 9092
# MAGIC KAFKA_TOPIC = 'bankserver1.bank.holding'
# MAGIC 
# MAGIC 
# MAGIC # Export environments variables (for shell usage)
# MAGIC import os
# MAGIC import subprocess
# MAGIC import sys
# MAGIC 
# MAGIC os.environ['EC2_IP'] = EC2_IP
# MAGIC os.environ['KAFKA_PORT'] = str(KAFKA_PORT)
# MAGIC os.environ['KAFKA_TOPIC'] = str(KAFKA_TOPIC)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 4.2.1 Create Bronze Table to gather data from Apache Kafka as it is

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Check if you have connection to EC2 - in my case I had to open AWS Security Group. (And add env variable ADVERTISED_HOST_NAME while creating Kafka)

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC # ping -c 1 $EC2_IP
# MAGIC # sudo apt-get install -y netcat > /dev/null
# MAGIC netcat -zv $EC2_IP $KAFKA_PORT
# MAGIC 
# MAGIC # sudo apt-get install -y kafkacat > /dev/null
# MAGIC # kafkacat -b $EC2_IP:$KAFKA_PORT -L
# MAGIC 
# MAGIC # Test from Other EC2 (in the same AWS subnet)
# MAGIC #export EC2_IP='10.142.225.168'
# MAGIC #export KAFKA_PORT=9092
# MAGIC #export KAFKA_TOPIC='bankserver1.bank.holding'
# MAGIC 
# MAGIC #apt update && apt install -y default-jdk
# MAGIC #java --version
# MAGIC #cd $(mktemp -d)
# MAGIC #wget https://dlcdn.apache.org/kafka/3.1.0/kafka_2.13-3.1.0.tgz
# MAGIC #tar -xzf kafka_2.13-3.1.0.tgz
# MAGIC #cd kafka_2.13-3.1.0
# MAGIC #bin/kafka-console-consumer.sh --topic $KAFKA_TOPIC --from-beginning --bootstrap-server $EC2_IP:$KAFKA_PORT

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # # Setup connection to Kafka
# MAGIC # (spark.readStream
# MAGIC #   .format("kafka")
# MAGIC #   .option("kafka.bootstrap.servers", f'{EC2_IP}:{KAFKA_PORT}')  # comma separated list of broker:host
# MAGIC #   .option("kafka.ssl.truststore.location", <dbfs-truststore-location>) \
# MAGIC #   .option("kafka.ssl.keystore.location", <dbfs-keystore-location>) \
# MAGIC #   .option("kafka.ssl.keystore.password", dbutils.secrets.get(scope=<certificate-scope-name>,key=<keystore-password-key-name>)) \
# MAGIC #   .option("kafka.ssl.truststore.password", dbutils.secrets.get(scope=<certificate-scope-name>,key=<truststore-password-key-name>))
# MAGIC #   .option("spark.kafka.consumer.cache.timeout", '10s')
# MAGIC #   .option("subscribe", KAFKA_TOPIC)                             # comma separated list of topics
# MAGIC #   .option("startingOffsets", "latest")                          # read data from the end of the stream 
# MAGIC #   .option("minPartitions", "1")  
# MAGIC #   .option("failOnDataLoss", "true")
# MAGIC 
# MAGIC df = spark \
# MAGIC   .readStream \
# MAGIC   .format("kafka") \
# MAGIC   .option("kafka.bootstrap.servers", f'{EC2_IP}:{KAFKA_PORT}') \
# MAGIC   .option("subscribe", KAFKA_TOPIC)   \
# MAGIC   .option("startingOffsets", "earliest") \
# MAGIC   .load() \
# MAGIC   .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp", "timestampType")
# MAGIC 
# MAGIC display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Depends if we will use casting value to string or not, we will see BASE64 or JSON.
# MAGIC 
# MAGIC ![kafka_read_stream_not_parsed.PNG](https://github.com/pgrabarczyk/databricks-sample/raw/master/images/Task4/kafka_read_stream_not_parsed.PNG)
# MAGIC 
# MAGIC ![kafka_read_stream_parsed.PNG](https://github.com/pgrabarczyk/databricks-sample/raw/master/images/Task4/kafka_read_stream_parsed.PNG)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now I'll make a sink to store the data in our Delta Lake

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC query = (df.writeStream
# MAGIC .format("delta")
# MAGIC .outputMode("append")
# MAGIC .option("mergeSchema", "true")
# MAGIC .option("checkpointLocation", checkpoint_bronze)
# MAGIC .trigger(once=True)
# MAGIC .start(table_path_bronze))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS Bronze_Bank_Holding_Unparsed;
# MAGIC DROP TABLE IF EXISTS Silver_Bank_Holding_Parsed;
# MAGIC DROP DATABASE IF EXISTS DB_Task_4;
# MAGIC CREATE DATABASE IF NOT EXISTS DB_Task_4;
# MAGIC USE DB_Task_4;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Ok, files inside S3 (`/bronze/Bank_Holding` start being created).
# MAGIC Now I'll create a Bronze Table and parse data to the Silver Table.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE Bronze_Bank_Holding_Unparsed
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/result_bucket/ireland-prod/1911096808398203/task_4/bronze/Bank_Holding";

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM Bronze_Bank_Holding_Unparsed;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 4.2.2 Stream and transform data from Bronze Table to Silver Table. Silver table should contain parsed data

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS Silver_Bank_Holding_Parsed;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS Silver_Bank_Holding_Parsed
# MAGIC (
# MAGIC   holding_id LONG,
# MAGIC   user_id LONG,
# MAGIC   holding_stock STRING,
# MAGIC   holding_quantity LONG,
# MAGIC   datetime_created TIMESTAMP,
# MAGIC   datetime_updated TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/mnt/result_bucket/ireland-prod/1911096808398203/task_4/silver/Bank_Holding';

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Time for transformation.
# MAGIC 
# MAGIC Check the json structure from Kafka.
# MAGIC 
# MAGIC **There are 3 options:**
# MAGIC 1. Rows can be created (When `before` is null and `after` is not empty)
# MAGIC 1. Rows can be updated (When both `before' and `after` are not empty)
# MAGIC 1. Rows can be deleted (When 'before' is not empty and 'after' is null)
# MAGIC 
# MAGIC **Note**
# MAGIC I don't care about timestamps in this example. In real case I probably should think how to solve more than 1 update/delete request.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Bank_Holding_Parsing_0 AS
# MAGIC SELECT json2.before, json2.after
# MAGIC FROM
# MAGIC (
# MAGIC   SELECT from_json(json.payload, "before String, after String") json2
# MAGIC   FROM (
# MAGIC     SELECT from_json(value, "payload String") json
# MAGIC     FROM Bronze_Bank_Holding_Unparsed
# MAGIC   )
# MAGIC );
# MAGIC 
# MAGIC -- Now I have two 'columns' in Bank_Holding_Parsing_0. "before" and "after"
# MAGIC 
# MAGIC -- 1. Rows can be created (When before is null and after is not empty)
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Bank_Holding_Parsing_Before_Null_After_Not_Null AS
# MAGIC SELECT before, after
# MAGIC FROM Bank_Holding_Parsing_0
# MAGIC WHERE before is NULL;
# MAGIC 
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Bank_Holding_Parsing_Inserts AS
# MAGIC SELECT json_inserts.holding_id, json_inserts.user_id, json_inserts.holding_stock, json_inserts.holding_quantity, json_inserts.datetime_created, json_inserts.datetime_updated
# MAGIC FROM
# MAGIC (
# MAGIC   SELECT from_json(after, "holding_id LONG, user_id LONG, holding_stock String, holding_quantity LONG, datetime_created TIMESTAMP, datetime_updated TIMESTAMP") json_inserts
# MAGIC   FROM Bank_Holding_Parsing_Before_Null_After_Not_Null
# MAGIC );
# MAGIC 
# MAGIC -- 2. Rows can be updated (When both before' andafter` are not empty)
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Bank_Holding_Parsing_Before_Not_Null_After_Not_Null AS
# MAGIC SELECT before, after
# MAGIC FROM Bank_Holding_Parsing_0
# MAGIC WHERE before is not NULL and after is not NULL;
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Bank_Holding_Parsing_Updates AS
# MAGIC SELECT json_inserts.holding_id, json_inserts.user_id, json_inserts.holding_stock, json_inserts.holding_quantity, json_inserts.datetime_created, json_inserts.datetime_updated
# MAGIC FROM
# MAGIC (
# MAGIC   SELECT from_json(after, "holding_id LONG, user_id LONG, holding_stock String, holding_quantity LONG, datetime_created TIMESTAMP, datetime_updated TIMESTAMP") json_inserts
# MAGIC   FROM Bank_Holding_Parsing_Before_Not_Null_After_Not_Null
# MAGIC );
# MAGIC 
# MAGIC -- 3. Rows can be deleted (When 'before' is not empty and 'after' is null)
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Bank_Holding_Parsing_Before_Not_Null_After_Null AS
# MAGIC SELECT before, after
# MAGIC FROM Bank_Holding_Parsing_0
# MAGIC WHERE after is NULL;
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Bank_Holding_Parsing_Deletes AS
# MAGIC SELECT json_deletes.holding_id
# MAGIC FROM
# MAGIC (
# MAGIC   SELECT from_json(after, "holding_id LONG") json_deletes
# MAGIC   FROM Bank_Holding_Parsing_Before_Not_Null_After_Null
# MAGIC );
# MAGIC 
# MAGIC -- Now I have three views for inserts, updates and deletes. First I'll execute upserts (inserts and updates)
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Bank_Holding_Parsing_Upserts AS
# MAGIC (
# MAGIC   SELECT * FROM Bank_Holding_Parsing_Inserts
# MAGIC   UNION ALL
# MAGIC   SELECT * FROM Bank_Holding_Parsing_Updates
# MAGIC );
# MAGIC 
# MAGIC MERGE INTO Silver_Bank_Holding_Parsed SILVER
# MAGIC   USING Bank_Holding_Parsing_Upserts TMP
# MAGIC   
# MAGIC   ON SILVER.holding_id = TMP.holding_id
# MAGIC   
# MAGIC   WHEN MATCHED THEN
# MAGIC     UPDATE SET *
# MAGIC   
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC --     INSERT *  -- I could use this instead of below
# MAGIC     INSERT (holding_id, user_id, holding_stock, holding_quantity, datetime_created, datetime_updated)
# MAGIC     VALUES (TMP.holding_id, TMP.user_id, TMP.holding_stock, TMP.holding_quantity, TMP.datetime_created, TMP.datetime_updated)
# MAGIC ;
# MAGIC     
# MAGIC -- Time for deletes. I could use just row SQL DELETE command ... but... why not using merge
# MAGIC MERGE INTO Silver_Bank_Holding_Parsed SILVER
# MAGIC   USING Bank_Holding_Parsing_Deletes TMP
# MAGIC   
# MAGIC   ON SILVER.holding_id = TMP.holding_id
# MAGIC   
# MAGIC   WHEN MATCHED THEN
# MAGIC     DELETE

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM Silver_Bank_Holding_Parsed;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 4.2.3 Stream and transform data from Silver Table to Gold Table. Gold table should contain data ready to consume by Amazon Redshift.
