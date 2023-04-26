# Databricks notebook source
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
dbfs_resources_path = f"/tmp/{username}/resources/"
os_dbfs_resources_path = f"/dbfs/" + dbfs_resources_path
spark.conf.set("c.database", username)
database=username

import os
os.environ['DBFS_RESOURCES_PATH'] = os_dbfs_resources_path
os.environ['DATABASE'] = database


# COMMAND ----------

# DBTITLE 1,Make a cloud directory for the Hadoop Data
# MAGIC %sh
# MAGIC # just to be sure, remove any existing data
# MAGIC rm -rf $DBFS_RESOURCES_PATH/data_from_hadoop;
# MAGIC mkdir $DBFS_RESOURCES_PATH/data_from_hadoop

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

# DBTITLE 1,Let's use autoloader  to load data into Delta
# Import functions
from pyspark.sql.functions import input_file_name, current_timestamp

# Define variables used in code below
file_path = dbfs_resources_path + "/data_from_hadoop"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"{database}.bronze_transactions"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"

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

# DBTITLE 1,Copy incremental data from HDFS to cloud storage
# MAGIC %sh
# MAGIC export HADOOP_HOME=/usr/local/hadoop/
# MAGIC export HIVE_HOME=/usr/local/hive/
# MAGIC export PATH=$HADOOP_HOME/bin:$HIVE_HOME/bin:$PATH
# MAGIC 
# MAGIC # NOTE we are copying data to the same location as before!
# MAGIC hadoop distcp /tmp/raw_transactions2 file:$DBFS_RESOURCES_PATH/data_from_hadoop

# COMMAND ----------

# DBTITLE 1,Incrementally load data into the bronze table - assumption is just an append, but we can change this to a merge
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

# MAGIC %sql
# MAGIC SELECT sum(balance), country_code FROM bronze_transactions GROUP BY country_code

# COMMAND ----------

# MAGIC %md
# MAGIC Handling Small files

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

# DBTITLE 1,VACUUM to remove older data files
# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM bronze_transactions retain 0 hours

# COMMAND ----------

# DBTITLE 1,File listing after VACUUM
# MAGIC %sh
# MAGIC ls -l /dbfs/user/hive/warehouse/${DATABASE}.db/bronze_transactions/

# COMMAND ----------


