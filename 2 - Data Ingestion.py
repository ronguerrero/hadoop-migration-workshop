# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestion data into the Delta Lakehouse
# MAGIC 
# MAGIC #### Objective
# MAGIC Learn a mechanism to load data into the Delta Lakehouse.  
# MAGIC Also look into Delta Live Tables - https://www.databricks.com/product/delta-live-tables  
# MAGIC 
# MAGIC #### Technologies Used
# MAGIC ##### Hadoop
# MAGIC * DistCP - to copy data from local HDFS to cloud storage
# MAGIC * Hive - to show content of data in HDFS 
# MAGIC ##### Databricks
# MAGIC * Auto Loader - Spark 
# MAGIC * SQL - Delta
# MAGIC 
# MAGIC   
# MAGIC #### Steps
# MAGIC * Migrate data from HDFS to cloud storage
# MAGIC * Use Auto Loader to load data into the Delta Lake
# MAGIC * Use Auto Loader to handle incremental loads into the Delta Lake
# MAGIC * Review how Delta handles small files
# MAGIC 
# MAGIC 
# MAGIC <img src ='https://github.com/ronguerrero/hadoop-migration-workshop/raw/main/resources/Ingestion%20-%20Data%20Flow.png'>

# COMMAND ----------

# DBTITLE 1,Set up environment variables for workshop 
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
dbfs_resources_path = f"/tmp/{username}/resources/"
os_dbfs_resources_path = f"/dbfs/" + dbfs_resources_path
spark.conf.set("c.database", username)
database=username

import os
os.environ['DBFS_RESOURCES_PATH'] = os_dbfs_resources_path
os.environ['DATABASE'] = database

# Import functions
from pyspark.sql.functions import input_file_name, current_timestamp

# Define variables used in code below
file_path = dbfs_resources_path + "/data_from_hadoop"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"{database}.bronze_transactions"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"

# COMMAND ----------

# DBTITLE 1,Make a cloud directory for the Hadoop Data
# MAGIC %sh
# MAGIC # just to be sure, remove any existing data
# MAGIC rm -rf $DBFS_RESOURCES_PATH/data_from_hadoop;
# MAGIC mkdir $DBFS_RESOURCES_PATH/data_from_hadoop

# COMMAND ----------

# MAGIC %md
# MAGIC <img src ='https://github.com/ronguerrero/hadoop-migration-workshop/raw/main/resources/Ingestion%20-%20Copy%20to%20Raw.png'>

# COMMAND ----------

# DBTITLE 1,Use DistCP to copy the data from HDFS to the newly create cloud location
# MAGIC %sh
# MAGIC export HADOOP_HOME=/usr/local/hadoop/
# MAGIC export HIVE_HOME=/usr/local/hive/
# MAGIC export PATH=$HADOOP_HOME/bin:$HIVE_HOME/bin:$PATH
# MAGIC hadoop distcp /tmp/raw_transactions file:$DBFS_RESOURCES_PATH/data_from_hadoop

# COMMAND ----------

# DBTITLE 1,Verify the data has been copied to cloud storage
# MAGIC %sh
# MAGIC ls $DBFS_RESOURCES_PATH/data_from_hadoop/raw_transactions/

# COMMAND ----------

# MAGIC %md
# MAGIC <img src ='https://github.com/ronguerrero/hadoop-migration-workshop/raw/main/resources/Ingestion%20-%20AutoLoad%20to%20Delta.png'>
# MAGIC 
# MAGIC Auto Loader Documentation - https://docs.databricks.com/ingestion/auto-loader/index.html

# COMMAND ----------

# DBTITLE 1,Let's use autoloader  to load data into Delta

# Clear out data from previous demo execution
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)

# Configure Auto Loader to ingest JSON data to a Delta table
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(file_path)
  .select("*", input_file_name().alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(table_name)).awaitTermination()

# COMMAND ----------

# DBTITLE 1,Check the row count 
# MAGIC %sql
# MAGIC USE ${c.database} ;
# MAGIC SELECT count(*) FROM bronze_transactions;

# COMMAND ----------

# DBTITLE 1,Spot check the data
# MAGIC %sql
# MAGIC SELECT * FROM bronze_transactions limit 10;

# COMMAND ----------

# DBTITLE 1,Review the incremental data from HDFS 
# MAGIC %sh
# MAGIC export HADOOP_HOME=/usr/local/hadoop/
# MAGIC export HIVE_HOME=/usr/local/hive/
# MAGIC export PATH=$HADOOP_HOME/bin:$HIVE_HOME/bin:$PATH
# MAGIC 
# MAGIC # NOTE we are copying data to the same location as before!
# MAGIC hdfs dfs -ls /tmp/raw_transactions2 
# MAGIC echo
# MAGIC # There are 3 json records we want appended
# MAGIC hdfs dfs -cat /tmp/raw_transactions2/part-00000-tid-4080381317870435782-30f504d1-326c-4189-8871-aecfe8e7e9e7-2241-1-c000.json

# COMMAND ----------

# MAGIC %md
# MAGIC <img src ='https://github.com/ronguerrero/hadoop-migration-workshop/raw/main/resources/Incremental%20-%20Copy%20to%20Raw.png'>

# COMMAND ----------

# DBTITLE 1,Copy incremental data from HDFS to cloud storage
# MAGIC %sh
# MAGIC export HADOOP_HOME=/usr/local/hadoop/
# MAGIC export HIVE_HOME=/usr/local/hive/
# MAGIC export PATH=$HADOOP_HOME/bin:$HIVE_HOME/bin:$PATH
# MAGIC 
# MAGIC # NOTE we are copying data to the same location as before!
# MAGIC hadoop distcp /tmp/raw_transactions2 file:$DBFS_RESOURCES_PATH/data_from_hadoop

# COMMAND ----------

# DBTITLE 1,Incrementally load data into the bronze table - assumption is just an append, but this can be changed to a merge
# load the incremental data
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(file_path)
  .select("*", input_file_name().alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(table_name)).awaitTermination()

# COMMAND ----------

# DBTITLE 1,Check row count - should see a minor increase in row count
# MAGIC %sql
# MAGIC SELECT count(*) FROM bronze_transactions;

# COMMAND ----------

# DBTITLE 1,Data in Databricks can be access via SQL queries
# MAGIC %sql
# MAGIC SELECT sum(balance), country_code FROM bronze_transactions GROUP BY country_code

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks Delta handles small files on ingest, and can compact files on demand - https://docs.databricks.com/delta/optimize.html

# COMMAND ----------

# DBTITLE 1,List current files for BRONZE_TRANSACTION table
# MAGIC %sh
# MAGIC ls -l /dbfs/user/hive/warehouse/${DATABASE}.db/bronze_transactions/

# COMMAND ----------

# DBTITLE 1,Run the OPTIMIZE command to compact files
# MAGIC %sql
# MAGIC OPTIMIZE bronze_transactions

# COMMAND ----------

# DBTITLE 1,File listing after OPTIMZE
# MAGIC %sh
# MAGIC ls -l /dbfs/user/hive/warehouse/${DATABASE}.db/bronze_transactions/

# COMMAND ----------

# MAGIC %md
# MAGIC The Delta VACUUM command removes data files for older revisions beyond a threshold time window.   
# MAGIC For more information - https://docs.databricks.com/sql/language-manual/delta-vacuum.html

# COMMAND ----------

# DBTITLE 1,VACUUM to remove older data files
# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM bronze_transactions retain 0 hours

# COMMAND ----------

# DBTITLE 1,File listing after VACUUM
# MAGIC %sh
# MAGIC ls -l /dbfs/user/hive/warehouse/${DATABASE}.db/bronze_transactions/

# COMMAND ----------


