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
# MAGIC I'll use [this blog post](https://www.startdataengineering.com/post/change-data-capture-using-debezium-kafka-and-pg/) to setup environment. Authors done great job there.
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
# MAGIC You can copy below scripts as files and execute them (e.g.: ```bash execute.sh```) or execute it line by line in your terminal.

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

#!/usr/bin/env bash
# execute.sh
set -eu

# Setup Postgres
docker run -d --name postgres -p 5432:5432 -e POSTGRES_USER=start_data_engineer -e POSTGRES_PASSWORD=password debezium/postgres:12

# Setup zookeeper & kafka
docker run -d --name zookeeper -p 2181:2181 -p 2888:2888 -p 3888:3888 debezium/zookeeper:1.1
docker run -d --name kafka -p 9092:9092 --link zookeeper:zookeeper debezium/kafka:1.1
            
docker run -d --name connect -p 8083:8083 --link kafka:kafka \
--link postgres:postgres -e BOOTSTRAP_SERVERS=kafka:9092 \
-e GROUP_ID=sde_group -e CONFIG_STORAGE_TOPIC=sde_storage_topic \
-e OFFSET_STORAGE_TOPIC=sde_offset_topic debezium/connect:1.1
                    
# curl -H "Accept:application/json" localhost:8083/connectors/

curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
localhost:8083/connectors/ -d '{"name": "sde-connector", "config": {"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "database.hostname": "postgres", "database.port": "5432", "database.user": "start_data_engineer", "database.password": "password", "database.dbname" : "start_data_engineer", "database.server.name": "bankserver1", "table.whitelist": "bank.holding"}}'
    
# curl -H "Accept:application/json" localhost:8083/connectors/

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Ok, we should be ready. Add some data to the DB. (Still executed from our EC2)

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ## Add data to Postgres
# MAGIC sudo apt-get install -y pgcli jq   
# MAGIC PGPASSWORD=password pgcli -h localhost -p 5432 -U start_data_engineer
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
# MAGIC 
# MAGIC # Test if data has been added
# MAGIC 
# MAGIC docker run -it --rm --name consumer --link zookeeper:zookeeper --link kafka:kafka debezium/kafka:1.1 watch-topic -a bankserver1.bank.holding --max-messages 1 | grep '^{' | jq

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC In my case I've JSON data with information that 1 row has been added (I deduced it from `before` and `after` parts).

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
# MAGIC dest_dir          = "dbfs:/mnt/result_bucket/ireland-prod/1884956493483554/task_4"
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
# MAGIC EC2_IP      = '10.225.181.154'
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
# MAGIC Check if you have connection to EC2 - in my case I had to open AWS Security Group.

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC # ping -c 1 $EC2_IP
# MAGIC sudo apt-get install -y netcat > /dev/null
# MAGIC netcat -zv $EC2_IP $KAFKA_PORT

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # # Setup connection to Kafka
# MAGIC # (spark.readStream
# MAGIC #   .format("kafka")
# MAGIC #   .option("kafka.bootstrap.servers", f'{EC2_IP}:{KAFKA_PORT}')  # comma separated list of broker:host
# MAGIC # #   .option("kafka.ssl.truststore.location", <dbfs-truststore-location>) \
# MAGIC # #   .option("kafka.ssl.keystore.location", <dbfs-keystore-location>) \
# MAGIC # #   .option("kafka.ssl.keystore.password", dbutils.secrets.get(scope=<certificate-scope-name>,key=<keystore-password-key-name>)) \
# MAGIC # #   .option("kafka.ssl.truststore.password", dbutils.secrets.get(scope=<certificate-scope-name>,key=<truststore-password-key-name>))
# MAGIC #   .option("spark.kafka.consumer.cache.timeout", '10s')
# MAGIC #   .option("subscribe", KAFKA_TOPIC)                             # comma separated list of topics
# MAGIC #   .option("startingOffsets", "latest")                          # read data from the end of the stream 
# MAGIC #   .option("minPartitions", "1")  
# MAGIC #   .option("failOnDataLoss", "true")
# MAGIC #   .load(table_path_bronze)                                      # f'{dest_dir}/bronze/Bank_Holding'
# MAGIC #   .createOrReplaceTempView('Bronze_Bank_Holding'))
# MAGIC 
# MAGIC df = spark \
# MAGIC   .readStream \
# MAGIC   .format("kafka") \
# MAGIC   .option("kafka.bootstrap.servers", f'{EC2_IP}:{KAFKA_PORT}') \
# MAGIC   .option("subscribe", KAFKA_TOPIC)   \
# MAGIC   .option("startingOffsets", "earliest") \
# MAGIC   .load()
# MAGIC #   .selectExpr()
# MAGIC 
# MAGIC df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
# MAGIC # display(df)
# MAGIC # df.printSchema()
# MAGIC # display(df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)"))

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC df.writeStream.format('console').start

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC sudo apt-get install -y kafkacat > /dev/null
# MAGIC kafkacat -b $EC2_IP:$KAFKA_PORT -L

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC telnet $EC2_IP $KAFKA_PORT

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM Bronze_Bank_Holding;
