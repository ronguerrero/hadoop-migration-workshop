# Databricks notebook source
# DBTITLE 1,Make a cloud directory for the Hadoop Data
# MAGIC %fs
# MAGIC mkdirs /data_from_hadoop

# COMMAND ----------

# DBTITLE 1,Use DistCP to copy the data from HDFS to the newly create cloud location
# MAGIC %sh
# MAGIC export HADOOP_HOME=/usr/local/hadoop/
# MAGIC export HIVE_HOME=/usr/local/hive/
# MAGIC export PATH=$HADOOP_HOME/bin:$HIVE_HOME/bin:$PATH
# MAGIC hadoop distcp /tmp/raw_transactions file:/dbfs/data_from_hadoop

# COMMAND ----------

# DBTITLE 1,Verify the data has been copied to cloud storage
# MAGIC %fs
# MAGIC ls /data_from_hadoop/raw_transactions/

# COMMAND ----------

# DBTITLE 1,Let's use autoloader  to load data into Delta
# Import functions
from pyspark.sql.functions import input_file_name, current_timestamp

# Define variables used in code below
file_path = "/data_from_hadoop"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"bronze_transactions"
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
  .toTable(table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_transactions limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(balance), country_code FROM bronze_transactions GROUP BY country_code

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /user/hive/warehouse/bronze_transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE bronze_transactions

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC vacuum bronze_transactions retain 0 hours

# COMMAND ----------


