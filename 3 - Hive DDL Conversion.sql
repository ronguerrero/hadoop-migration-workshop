-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC %md
-- MAGIC ### Convert Hive DDL to Databricks
-- MAGIC 
-- MAGIC #### Technologies Used
-- MAGIC ##### Hadoop
-- MAGIC * Hive - to show content of data in HDFS 
-- MAGIC ##### Databricks
-- MAGIC * SQL
-- MAGIC 
-- MAGIC   
-- MAGIC #### Objectives
-- MAGIC * Review and existing Hive DDL statement
-- MAGIC * Demonstrate how existing Hive DDL statements can be run in Databricks
-- MAGIC * Highlight recommended changes to Hive DDL statements
-- MAGIC * Options to convert data into Delta Format (beyond Auto Loader)
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC #### DDL Conversion Considerations
-- MAGIC 
-- MAGIC Majority of data types align between Hive / Impala and Databricks SQL
-- MAGIC 
-- MAGIC Databricks SQL Data Types - https://docs.databricks.com/sql/language-manual/sql-ref-datatypes.html   
-- MAGIC Hive Data Types - https://cwiki.apache.org/confluence/display/hive/languagemanual+types  
-- MAGIC Impala Data Types - https://impala.apache.org/docs/build/plain-html/topics/impala_datatypes.html
-- MAGIC 
-- MAGIC Existing partitioning strategy can be maintained - https://docs.databricks.com/sql/language-manual/sql-ref-partition.html#partitions  
-- MAGIC Databricks SQL Partitioning Best Practices - https://docs.databricks.com/tables/partitions.html

-- COMMAND ----------

-- MAGIC %python
-- MAGIC username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
-- MAGIC dbfs_resources_path = f"/tmp/{username}/resources/"
-- MAGIC os_dbfs_resources_path = f"/dbfs/" + dbfs_resources_path
-- MAGIC spark.conf.set("c.database", username)
-- MAGIC database=username
-- MAGIC 
-- MAGIC import os
-- MAGIC os.environ['DBFS_RESOURCES_PATH'] = os_dbfs_resources_path
-- MAGIC os.environ['DATABASE'] = database

-- COMMAND ----------

-- DBTITLE 1,Generate the Hive DDL for the Loan Transaction Table
-- MAGIC %sh
-- MAGIC export HADOOP_HOME=/usr/local/hadoop/
-- MAGIC export HIVE_HOME=/usr/local/hive/
-- MAGIC export PATH=$HADOOP_HOME/bin:$HIVE_HOME/bin:$PATH
-- MAGIC cd $HIVE_HOME
-- MAGIC 
-- MAGIC hive -e "SHOW CREATE TABLE TRANSACTIONS_PARQUET"

-- COMMAND ----------

-- DBTITLE 1,Copy-Pasted DDL from above, with minor changes
-- let's drop the table just in case it exists
USE ${c.database};
DROP TABLE IF EXISTS TRANSACTIONS_PARQUET_HADOOP;

-- DDL is the same, but we'll remove the LOCATION clause to keep things simple
CREATE TABLE `TRANSACTIONS_PARQUET_HADOOP`(
  `acc_fv_change_before_taxes` float, 
  `accounting_treatment_id` float, 
  `accrued_interest` float, 
  `arrears_balance` float, 
  `balance` float, 
  `base_rate` string, 
  `behavioral_curve_id` float, 
  `cost_center_code` string, 
  `count` float, 
  `country_code` string, 
  `dte` string, 
  `encumbrance_type` string, 
  `end_date` string, 
  `first_payment_date` string, 
  `guarantee_scheme` string, 
  `id` float, 
  `imit_amount` float, 
  `last_payment_date` string, 
  `minimum_balance_eur` float, 
  `next_payment_date` string, 
  `purpose` string, 
  `status` string, 
  `type` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
--LOCATION - to keep things simple, lets make this a managed table
--
TBLPROPERTIES (
  'transient_lastDdlTime'='1681328780')


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Recommended changes
-- MAGIC &nbsp;
-- MAGIC 1) Convert to Spark SQL syntax - USING clause vs STORED AS  
-- MAGIC 2) Leverage DELTA format  
-- MAGIC 3) Re-review partioning strategy - https://docs.databricks.com/tables/partitions.html#:~:text=Databricks%20recommends%20all%20partitions%20contain,tables%20with%20many%20smaller%20partitions.

-- COMMAND ----------

-- DBTITLE 1,DDL converted to Spark SQL syntax - Recommended
DROP TABLE IF EXISTS TRANSACTIONS_DELTA;
CREATE TABLE `TRANSACTIONS_DELTA`(
  `acc_fv_change_before_taxes` float, 
  `accounting_treatment_id` float, 
  `accrued_interest` float, 
  `arrears_balance` float, 
  `balance` float, 
  `base_rate` string, 
  `behavioral_curve_id` float, 
  `cost_center_code` string, 
  `count` float, 
  `country_code` string, 
  `dte` string, 
  `encumbrance_type` string, 
  `end_date` string, 
  `first_payment_date` string, 
  `guarantee_scheme` string, 
  `id` float, 
  `imit_amount` float, 
  `last_payment_date` string, 
  `minimum_balance_eur` float, 
  `next_payment_date` string, 
  `purpose` string, 
  `status` string, 
  `type` string)
USING DELTA
TBLPROPERTIES (
  'transient_lastDdlTime'='1681328780')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We saw how you can directly ingest data into the Delta Lake using Auto Loader.  
-- MAGIC Databricks can also read data from a Parquet (or any common data format) table and write into a Delta version.  
-- MAGIC The ORC format is also supported.     
-- MAGIC Converting to the Delta format will provide the best performance and ease of data management in the cloud.

-- COMMAND ----------

-- DBTITLE 1,In Databricks, show data from existing parquet table
SELECT * FROM TRANSACTIONS_PARQUET;

-- COMMAND ----------

-- DBTITLE 1,Load the data from the Parquet table into the Delta table we defined earlier
-- MAGIC %sql
-- MAGIC INSERT INTO TRANSACTIONS_DELTA (SELECT * FROM TRANSACTIONS_PARQUET);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You can also convert in-place, existing Parquet tables into Delta.

-- COMMAND ----------

-- DBTITLE 1,Let's start by creating a Parquet table we can convert
DROP TABLE IF EXISTS TRANSACTIONS_CONVERTED;
CREATE TABLE TRANSACTIONS_CONVERTED LIKE TRANSACTIONS_PARQUET USING PARQUET;
INSERT INTO TRANSACTIONS_CONVERTED SELECT * FROM TRANSACTIONS_PARQUET;
SELECT * FROM TRANSACTIONS_CONVERTED;

-- COMMAND ----------

-- DBTITLE 1,Confirm the table is in Parquet format - scroll down to "Provider" value
DESCRIBE EXTENDED TRANSACTIONS_CONVERTED;

-- COMMAND ----------

-- DBTITLE 1,Show the underlying contents on cloud storage
-- MAGIC %sh
-- MAGIC ls /dbfs/user/hive/warehouse/${DATABASE}.db/transactions_converted/

-- COMMAND ----------

-- DBTITLE 1,Simply run the CONVERT TO DELTA command to convert a parquet table to the delta format.
CONVERT TO DELTA TRANSACTIONS_CONVERTED;

-- COMMAND ----------

-- DBTITLE 1,Confirm the table is now in Delta format - scroll down to "Provider" value
DESCRIBE EXTENDED TRANSACTIONS_CONVERTED;

-- COMMAND ----------

-- DBTITLE 1,Note how there is a delta_log directory within the table data
-- MAGIC %sh
-- MAGIC ls /dbfs/user/hive/warehouse/${DATABASE}.db/transactions_converted/
