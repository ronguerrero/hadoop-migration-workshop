-- Databricks notebook source
-- DBTITLE 1,Hive - Table Setup
-- MAGIC %md
-- MAGIC insert
-- MAGIC update
-- MAGIC delete
-- MAGIC merge

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

-- DBTITLE 1,Let's look at the original data in Hive
-- MAGIC %sh
-- MAGIC export HADOOP_HOME=/usr/local/hadoop/
-- MAGIC export HIVE_HOME=/usr/local/hive/
-- MAGIC export PATH=$HADOOP_HOME/bin:$HIVE_HOME/bin:$PATH
-- MAGIC cd $HIVE_HOME
-- MAGIC 
-- MAGIC # We will work with 2 tables:
-- MAGIC #    a) Original table with existing data - RAW_TRANSACTIONS
-- MAGIC #    b) Table with new incremental data (updates/insert) - RAW_TRANSACTIONS_NEW
-- MAGIC 
-- MAGIC hive -e "
-- MAGIC SELECT COUNT(1) FROM RAW_TRANSACTIONS;
-- MAGIC SELECT COUNT(1) FROM RAW_TRANSACTIONS_NEW;
-- MAGIC SELECT * FROM RAW_TRANSACTIONS limit 10;
-- MAGIC SELECT * FROM RAW_TRANSACTIONS_NEW limit 3"

-- COMMAND ----------

-- DBTITLE 1,Hive Parquet - Handle Merge of New Data
-- MAGIC %sh
-- MAGIC export HADOOP_HOME=/usr/local/hadoop/
-- MAGIC export HIVE_HOME=/usr/local/hive/
-- MAGIC export PATH=$HADOOP_HOME/bin:$HIVE_HOME/bin:$PATH
-- MAGIC cd $HIVE_HOME
-- MAGIC 
-- MAGIC hive -e "DROP TABLE IF EXISTS TRANSACTIONS_PARQUET_NEW;
-- MAGIC          CREATE TABLE TRANSACTIONS_PARQUET_NEW LIKE TRANSACTIONS_PARQUET;
-- MAGIC          INSERT INTO TRANSACTIONS_PARQUET_NEW (
-- MAGIC             -- first get all the existing rows in TRANSACTIONS_PARQUET
-- MAGIC             SELECT * FROM TRANSACTIONS_PARQUET 
-- MAGIC                 WHERE ID NOT IN (SELECT ID FROM RAW_TRANSACTIONS_NEW)
-- MAGIC          UNION
-- MAGIC             -- add all the rows in RAW_TRANSACTIONS_NEW, this is the INSERT/UPDATE scenario
-- MAGIC              SELECT * FROM RAW_TRANSACTIONS_NEW);
-- MAGIC          ALTER TABLE TRANSACTIONS_PARQUET RENAME TO TRANSACTIONS_PARQUET_OLD;
-- MAGIC          ALTER TABLE TRANSACTIONS_PARQUET_NEW RENAME TO TRANSACTIONS_PARQUET;"

-- COMMAND ----------

-- DBTITLE 1,Hive - Validate existing data
-- MAGIC %sh
-- MAGIC export HADOOP_HOME=/usr/local/hadoop/
-- MAGIC export HIVE_HOME=/usr/local/hive/
-- MAGIC export PATH=$HADOOP_HOME/bin:$HIVE_HOME/bin:$PATH
-- MAGIC cd $HIVE_HOME
-- MAGIC 
-- MAGIC hive -e "SELECT count(*) FROM TRANSACTIONS_PARQUET"

-- COMMAND ----------

-- DBTITLE 1,Databricks - Identical SQL statements as Hive.    Works, but we can do better.
USE ${c.database};
DROP TABLE IF EXISTS TRANSACTIONS_PARQUET_OLD;
DROP TABLE IF EXISTS TRANSACTIONS_PARQUET_NEW;
CREATE TABLE TRANSACTIONS_PARQUET_NEW LIKE TRANSACTIONS_PARQUET;
INSERT INTO
  TRANSACTIONS_PARQUET_NEW (
    SELECT
      *
    FROM
      TRANSACTIONS_PARQUET
    WHERE ID NOT IN (
        SELECT
          ID
        FROM
          RAW_TRANSACTIONS_NEW
      )
    UNION
    SELECT
      *
    FROM
      RAW_TRANSACTIONS_NEW
  );
ALTER TABLE
  TRANSACTIONS_PARQUET RENAME TO TRANSACTIONS_PARQUET_OLD;
ALTER TABLE
  TRANSACTIONS_PARQUET_NEW RENAME TO TRANSACTIONS_PARQUET;

-- COMMAND ----------

SELECT count(*) FROM TRANSACTIONS_PARQUET;

-- COMMAND ----------

-- DBTITLE 1,Databricks - Use MERGE logic to simplify code with Delta Tables!
MERGE INTO 
   TRANSACTIONS_DELTA 
USING 
   RAW_TRANSACTIONS_NEW
ON 
   TRANSACTIONS_DELTA.ID = RAW_TRANSACTIONS_NEW.ID
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
     INSERT  *

-- COMMAND ----------

SELECT COUNT(1) FROM TRANSACTIONS_DELTA;

-- COMMAND ----------


