# Databricks notebook source
# MAGIC %md
# MAGIC #DDL Conversion Considerations
# MAGIC 
# MAGIC Majority of data types align between Hive / Impala and Databricks SQL
# MAGIC 
# MAGIC Databricks SQL Data Types - https://docs.databricks.com/sql/language-manual/sql-ref-datatypes.html   
# MAGIC Hive Data Types - https://cwiki.apache.org/confluence/display/hive/languagemanual+types  
# MAGIC Impala Data Types - https://impala.apache.org/docs/build/plain-html/topics/impala_datatypes.html
# MAGIC 
# MAGIC Existing partitioning strategy can be maintained - https://docs.databricks.com/sql/language-manual/sql-ref-partition.html#partitions  
# MAGIC Databricks SQL Partitioning Best Practices - https://docs.databricks.com/tables/partitions.html

# COMMAND ----------

# DBTITLE 1,Generate the Hive DDL for the Loan Transaction Table
# MAGIC %sh
# MAGIC export HADOOP_HOME=/usr/local/hadoop/
# MAGIC export HIVE_HOME=/usr/local/hive/
# MAGIC export PATH=$HADOOP_HOME/bin:$HIVE_HOME/bin:$PATH
# MAGIC cd $HIVE_HOME
# MAGIC 
# MAGIC hive -e "SHOW CREATE TABLE TRANSACTIONS_PARQUET"

# COMMAND ----------

# DBTITLE 1,Copy-Pasted DDL from above, with minor changes
# MAGIC %sql
# MAGIC -- let's drop the table just in case it exists
# MAGIC DROP TABLE IF EXISTS TRANSACTIONS_PARQUET_HADOOP;
# MAGIC 
# MAGIC -- DDL is the same, but we'll remove the LOCATION clause to keep things simple
# MAGIC CREATE TABLE `TRANSACTIONS_PARQUET_HADOOP`(
# MAGIC   `acc_fv_change_before_taxes` float, 
# MAGIC   `accounting_treatment_id` float, 
# MAGIC   `accrued_interest` float, 
# MAGIC   `arrears_balance` float, 
# MAGIC   `balance` float, 
# MAGIC   `base_rate` string, 
# MAGIC   `behavioral_curve_id` float, 
# MAGIC   `cost_center_code` string, 
# MAGIC   `count` float, 
# MAGIC   `country_code` string, 
# MAGIC   `dte` string, 
# MAGIC   `encumbrance_type` string, 
# MAGIC   `end_date` string, 
# MAGIC   `first_payment_date` string, 
# MAGIC   `guarantee_scheme` string, 
# MAGIC   `id` float, 
# MAGIC   `imit_amount` float, 
# MAGIC   `last_payment_date` string, 
# MAGIC   `minimum_balance_eur` float, 
# MAGIC   `next_payment_date` string, 
# MAGIC   `purpose` string, 
# MAGIC   `status` string, 
# MAGIC   `type` string)
# MAGIC ROW FORMAT SERDE 
# MAGIC   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
# MAGIC STORED AS INPUTFORMAT 
# MAGIC   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
# MAGIC OUTPUTFORMAT 
# MAGIC   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
# MAGIC --LOCATION - to keep things simple, lets make this a managed table
# MAGIC --
# MAGIC TBLPROPERTIES (
# MAGIC   'transient_lastDdlTime'='1681328780')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recommended changes
# MAGIC &nbsp;
# MAGIC 1) Convert to Spark SQL syntax - USING clause vs STORED AS  
# MAGIC 2) Leverage DELTA format  
# MAGIC 3) Rethink partitioning strategy - https://docs.databricks.com/tables/partitions.html#:~:text=Databricks%20recommends%20all%20partitions%20contain,tables%20with%20many%20smaller%20partitions.

# COMMAND ----------

username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
dbfs_raw_path = f"/tmp/{username}/raw_transactions/"
dbfs_raw_new_path = f"/tmp/{username}/raw_transactions_new/"


# COMMAND ----------

# DBTITLE 1,DDL converted to Spark SQL syntax - Recommended
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS TRANSACTIONS_DELTA;
# MAGIC CREATE TABLE `TRANSACTIONS_DELTA`(
# MAGIC   `acc_fv_change_before_taxes` float, 
# MAGIC   `accounting_treatment_id` float, 
# MAGIC   `accrued_interest` float, 
# MAGIC   `arrears_balance` float, 
# MAGIC   `balance` float, 
# MAGIC   `base_rate` string, 
# MAGIC   `behavioral_curve_id` float, 
# MAGIC   `cost_center_code` string, 
# MAGIC   `count` float, 
# MAGIC   `country_code` string, 
# MAGIC   `dte` string, 
# MAGIC   `encumbrance_type` string, 
# MAGIC   `end_date` string, 
# MAGIC   `first_payment_date` string, 
# MAGIC   `guarantee_scheme` string, 
# MAGIC   `id` float, 
# MAGIC   `imit_amount` float, 
# MAGIC   `last_payment_date` string, 
# MAGIC   `minimum_balance_eur` float, 
# MAGIC   `next_payment_date` string, 
# MAGIC   `purpose` string, 
# MAGIC   `status` string, 
# MAGIC   `type` string)
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'transient_lastDdlTime'='1681328780')

# COMMAND ----------

# MAGIC %md
# MAGIC We saw how you an directly ingest data into the Delta Lake using Auto Loader.  
# MAGIC Databricks can also read data from a Parquet table and write into a Delta version

# COMMAND ----------

# DBTITLE 1,In Databricks, show data from existing parquet table
# MAGIC %sql
# MAGIC SELECT * FROM TRANSACTIONS_PARQUET;

# COMMAND ----------

# DBTITLE 1,Load the data from the Parquet table into the Delta table we defined earlier/
# MAGIC %sql
# MAGIC INSERT INTO TRANSACTIONS_DELTA (SELECT * FROM TRANSACTIONS_PARQUET);

# COMMAND ----------

# MAGIC %md
# MAGIC You can also convert in-place, existing Parquet tables into Delta.

# COMMAND ----------

# DBTITLE 1,Let's start by creating a Parquet table we can convert
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS TRANSACTIONS_CONVERTED;
# MAGIC CREATE TABLE TRANSACTIONS_CONVERTED LIKE TRANSACTIONS_PARQUET USING PARQUET;
# MAGIC INSERT INTO TRANSACTIONS_CONVERTED SELECT * FROM TRANSACTIONS_PARQUET;
# MAGIC SELECT * FROM TRANSACTIONS_CONVERTED;

# COMMAND ----------

# DBTITLE 1,Simply run the CONVERT TO DELTA command to convert a parquet table to the delta format.
# MAGIC %sql
# MAGIC CONVERT TO DELTA TRANSACTIONS_CONVERTED;

# COMMAND ----------


