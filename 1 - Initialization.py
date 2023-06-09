# Databricks notebook source
# MAGIC %md
# MAGIC ### Workshop Setup
# MAGIC 
# MAGIC #### Objective
# MAGIC Spin up a local hadoop environment for future exercises.
# MAGIC Download and initialize lab data.    
# MAGIC 
# MAGIC #### Technologies Used
# MAGIC ##### Hadoop
# MAGIC * HDFS
# MAGIC * YARN
# MAGIC * Hive
# MAGIC ##### Databricks
# MAGIC * SQL - Delta
# MAGIC 
# MAGIC 
# MAGIC   
# MAGIC #### Steps
# MAGIC * Deploy Hadoop in Pseudo-Distributed mode
# MAGIC * Set up Hive environment - data and UDF
# MAGIC * Download data to cloud storage (DBFS)
# MAGIC * Create initial Delta tables
# MAGIC 
# MAGIC 
# MAGIC <img src ='https://github.com/ronguerrero/hadoop-migration-workshop/raw/main/resources/Lab%20Environment.png'>

# COMMAND ----------

# DBTITLE 1,Install a pseudo-distributed Hadoop Environment with Hive
# MAGIC %sh
# MAGIC cd /root/
# MAGIC # install hadoop in pseudo-distributed mode
# MAGIC wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz
# MAGIC tar -xzvf hadoop-3.3.1.tar.gz
# MAGIC export HADOOP_HOME=/usr/local/hadoop
# MAGIC rm -rf $HADOOP_HOME
# MAGIC mv hadoop-3.3.1 $HADOOP_HOME
# MAGIC export PATH=$HADOOP_HOME/bin:$PATH
# MAGIC 
# MAGIC # set the java home
# MAGIC sed -i.bak "s@# export JAVA_HOME=@export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")@g" $HADOOP_HOME/etc/hadoop/hadoop-env.sh
# MAGIC 
# MAGIC # allow passphraseless ssh
# MAGIC rm -f ~/.ssh/id_rsa
# MAGIC ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
# MAGIC cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
# MAGIC chmod 0600 ~/.ssh/authorized_keys
# MAGIC 
# MAGIC # configure hadoop defaults
# MAGIC echo "<configuration>
# MAGIC     <property>
# MAGIC         <name>fs.defaultFS</name>
# MAGIC         <value>hdfs://localhost:9000</value>
# MAGIC     </property>
# MAGIC </configuration>" > $HADOOP_HOME/etc/hadoop/core-site.xml
# MAGIC echo "<configuration>
# MAGIC     <property>
# MAGIC         <name>dfs.replication</name>
# MAGIC         <value>1</value>
# MAGIC     </property>
# MAGIC </configuration>" >  $HADOOP_HOME/etc/hadoop/hdfs-site.xml
# MAGIC 
# MAGIC # set the default environment variables
# MAGIC export HDFS_NAMENODE_USER="root"
# MAGIC export HDFS_DATANODE_USER="root"
# MAGIC export HDFS_SECONDARYNAMENODE_USER="root"
# MAGIC export YARN_RESOURCEMANAGER_USER="root"
# MAGIC export YARN_NODEMANAGER_USER="root"
# MAGIC export YARN_NODEMANAGER_USER="root"
# MAGIC export HADOOP_MAPRED_HOME=$HADOOP_HOME
# MAGIC 
# MAGIC # start hdfs
# MAGIC $HADOOP_HOME/bin/hdfs namenode -format
# MAGIC $HADOOP_HOME/sbin/start-dfs.sh
# MAGIC 
# MAGIC # install hive
# MAGIC wget https://dlcdn.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
# MAGIC tar -xzvf apache-hive-3.1.3-bin.tar.gz 
# MAGIC export HIVE_HOME=/usr/local/hive
# MAGIC rm -rf $HIVE_HOME
# MAGIC mv apache-hive-3.1.3-bin $HIVE_HOME
# MAGIC export PATH=$HIVE_HOME/bin:$PATH
# MAGIC cp $HIVE_HOME/conf/hive-env.sh.template $HIVE_HOME/conf/hive-env.sh
# MAGIC cd $HIVE_HOME
# MAGIC schematool -dbType derby -initSchema
# MAGIC 
# MAGIC # configure yarn
# MAGIC echo "<configuration>
# MAGIC     <property>
# MAGIC         <name>mapreduce.framework.name</name>
# MAGIC         <value>yarn</value>
# MAGIC     </property>
# MAGIC     <property>
# MAGIC         <name>mapreduce.application.classpath</name>
# MAGIC         <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*:$HIVE_HOME/lib/*</value>
# MAGIC     </property>
# MAGIC </configuration>" >  $HADOOP_HOME/etc/hadoop/mapred-site.xml
# MAGIC 
# MAGIC echo "<configuration>
# MAGIC     <property>
# MAGIC         <name>yarn.nodemanager.aux-services</name>
# MAGIC         <value>mapreduce_shuffle</value>
# MAGIC     </property>
# MAGIC     <property>
# MAGIC         <name>yarn.nodemanager.env-whitelist</name>
# MAGIC <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value>
# MAGIC     </property>
# MAGIC </configuration>" > $HADOOP_HOME/etc/hadoop/yarn-site.xml
# MAGIC 
# MAGIC # start yarn
# MAGIC $HADOOP_HOME/sbin/start-yarn.sh

# COMMAND ----------

# DBTITLE 1,Initialize Hive Data and UDF
# MAGIC %sh
# MAGIC export HADOOP_HOME=/usr/local/hadoop/
# MAGIC export HIVE_HOME=/usr/local/hive/
# MAGIC export PATH=$HADOOP_HOME/bin:$HIVE_HOME/bin:$PATH
# MAGIC cd $HIVE_HOME
# MAGIC 
# MAGIC # download the raw transaction data
# MAGIC wget https://github.com/ronguerrero/hadoop-utilities/raw/main/resources/raw_transactions.tgz
# MAGIC tar zxvf raw_transactions.tgz
# MAGIC 
# MAGIC # download the second set of transaction data
# MAGIC wget https://github.com/ronguerrero/hadoop-utilities/raw/main/resources/raw_transactions2.tgz
# MAGIC tar zxvf raw_transactions2.tgz
# MAGIC 
# MAGIC 
# MAGIC #upload to hdfs
# MAGIC hadoop fs -mkdir /tmp
# MAGIC hadoop fs -copyFromLocal raw_transactions hdfs:/tmp/
# MAGIC hadoop fs -copyFromLocal raw_transactions2 hdfs:/tmp/
# MAGIC 
# MAGIC 
# MAGIC # create the raw json table
# MAGIC hive -e "CREATE EXTERNAL TABLE RAW_TRANSACTIONS (
# MAGIC acc_fv_change_before_taxes float,
# MAGIC accounting_treatment_id float,
# MAGIC accrued_interest float,
# MAGIC arrears_balance float,
# MAGIC balance float,
# MAGIC base_rate string,
# MAGIC behavioral_curve_id float,
# MAGIC cost_center_code string,
# MAGIC count float,
# MAGIC country_code string,
# MAGIC dte string,
# MAGIC encumbrance_type string,
# MAGIC end_date string,
# MAGIC first_payment_date string,
# MAGIC guarantee_scheme string,
# MAGIC id float,
# MAGIC imit_amount float,
# MAGIC last_payment_date string,
# MAGIC minimum_balance_eur float,
# MAGIC next_payment_date string,
# MAGIC purpose string,
# MAGIC status string,
# MAGIC type string)
# MAGIC ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
# MAGIC LOCATION '/tmp/raw_transactions'"
# MAGIC 
# MAGIC hive -e "CREATE EXTERNAL TABLE RAW_TRANSACTIONS_NEW (
# MAGIC acc_fv_change_before_taxes float,
# MAGIC accounting_treatment_id float,
# MAGIC accrued_interest float,
# MAGIC arrears_balance float,
# MAGIC balance float,
# MAGIC base_rate string,
# MAGIC behavioral_curve_id float,
# MAGIC cost_center_code string,
# MAGIC count float,
# MAGIC country_code string,
# MAGIC dte string,
# MAGIC encumbrance_type string,
# MAGIC end_date string,
# MAGIC first_payment_date string,
# MAGIC guarantee_scheme string,
# MAGIC id float,
# MAGIC imit_amount float,
# MAGIC last_payment_date string,
# MAGIC minimum_balance_eur float,
# MAGIC next_payment_date string,
# MAGIC purpose string,
# MAGIC status string,
# MAGIC type string)
# MAGIC ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
# MAGIC LOCATION '/tmp/raw_transactions2'"
# MAGIC 
# MAGIC 
# MAGIC # create a parquet version of the data
# MAGIC hive -e "
# MAGIC create table transactions_parquet like raw_transactions stored as parquet;
# MAGIC insert into transactions_parquet select * from raw_transactions"
# MAGIC 
# MAGIC # download and register the the udf
# MAGIC wget https://github.com/ronguerrero/HiveUDF/raw/main/target/hive-udf-1.0-SNAPSHOT.jar
# MAGIC cp hive-udf-1.0-SNAPSHOT.jar $HIVE_HOME/lib
# MAGIC hive -e "CREATE FUNCTION udftypeof AS 'com.mycompany.hiveudf.TypeOf'"

# COMMAND ----------

# DBTITLE 1,Verify Data is Available, and UDF works
# MAGIC %sh
# MAGIC export HADOOP_HOME=/usr/local/hadoop/
# MAGIC export HIVE_HOME=/usr/local/hive/
# MAGIC export PATH=$HADOOP_HOME/bin:$HIVE_HOME/bin:$PATH
# MAGIC cd $HIVE_HOME
# MAGIC 
# MAGIC hive -S -e "
# MAGIC select * from transactions_parquet limit 1;
# MAGIC select udftypeof(acc_fv_change_before_taxes) from transactions_parquet limit 1"

# COMMAND ----------

# DBTITLE 1,Download the data for the workshop
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
dbfs_raw_path = f"/tmp/{username}/raw_transactions/"
dbfs_raw_new_path = f"/tmp/{username}/raw_transactions_new/"

dbutils.fs.mkdirs(dbfs_raw_path)
dbutils.fs.mkdirs(dbfs_raw_new_path)

dbutils.fs.rm(dbfs_raw_path, True)
dbutils.fs.rm(dbfs_raw_new_path, True)

dbutils.fs.cp("file:/usr/local/hive/raw_transactions/", dbfs_raw_path, True)
dbutils.fs.cp("file:/usr/local/hive/raw_transactions2/", dbfs_raw_new_path, True)

spark.conf.set("c.dbfs_raw_path", dbfs_raw_path)
spark.conf.set("c.dbfs_raw_new_path", dbfs_raw_new_path)
spark.conf.set("c.database", username)

dbfs_resources_path = f"/tmp/{username}/resources/"
os_dbfs_resources_path = f"/dbfs/tmp/{username}/resources/"
dbutils.fs.mkdirs(dbfs_resources_path)

import os
os.environ['DBFS_RESOURCES_PATH'] = os_dbfs_resources_path
print(os.getenv('DBFS_RESOURCES_PATH'))

# COMMAND ----------

# DBTITLE 1,Create the Delta Tables for the workshop
# MAGIC %sql
# MAGIC CREATE DATABASE if not exists ${c.database};
# MAGIC USE ${c.database};
# MAGIC DROP TABLE IF EXISTS RAW_TRANSACTIONS;
# MAGIC CREATE TABLE RAW_TRANSACTIONS (
# MAGIC acc_fv_change_before_taxes float,
# MAGIC accounting_treatment_id float,
# MAGIC accrued_interest float,
# MAGIC arrears_balance float,
# MAGIC balance float,
# MAGIC base_rate string,
# MAGIC behavioral_curve_id float,
# MAGIC cost_center_code string,
# MAGIC count float,
# MAGIC country_code string,
# MAGIC dte string,
# MAGIC encumbrance_type string,
# MAGIC end_date string,
# MAGIC first_payment_date string,
# MAGIC guarantee_scheme string,
# MAGIC id float,
# MAGIC imit_amount float,
# MAGIC last_payment_date string,
# MAGIC minimum_balance_eur float,
# MAGIC next_payment_date string,
# MAGIC purpose string,
# MAGIC status string,
# MAGIC type string)
# MAGIC USING JSON
# MAGIC LOCATION '${c.dbfs_raw_path}';
# MAGIC DROP TABLE IF EXISTS RAW_TRANSACTIONS_NEW;
# MAGIC CREATE TABLE RAW_TRANSACTIONS_NEW LIKE RAW_TRANSACTIONS LOCATION '${c.dbfs_raw_new_path}';
# MAGIC 
# MAGIC DROP TABLE IF EXISTS TRANSACTIONS_PARQUET;
# MAGIC CREATE TABLE TRANSACTIONS_PARQUET LIKE RAW_TRANSACTIONS USING PARQUET;
# MAGIC INSERT INTO TRANSACTIONS_PARQUET SELECT * FROM RAW_TRANSACTIONS;

# COMMAND ----------

# DBTITLE 1,Validate the the new transaction data
# MAGIC %sql
# MAGIC SELECT count(*) FROM RAW_TRANSACTIONS_NEW;

# COMMAND ----------

# DBTITLE 1,Download test data for Spark examples
# MAGIC %sh
# MAGIC cd /tmp
# MAGIC rm people.*
# MAGIC wget https://github.com/ronguerrero/hadoop-utilities/raw/main/resources/people.json 
# MAGIC wget https://github.com/ronguerrero/hadoop-utilities/raw/main/resources/people.txt 

# COMMAND ----------

dbutils.fs.cp("file:/tmp/people.json", dbfs_resources_path)
dbutils.fs.cp("file:/tmp/people.txt", dbfs_resources_path)

# COMMAND ----------

# DBTITLE 1,Spark JAR examples
# MAGIC %sh
# MAGIC cd /tmp
# MAGIC wget https://github.com/ronguerrero/hadoop-utilities/raw/main/resources/original-spark-examples_2.12-3.3.0.jar
# MAGIC wget https://github.com/ronguerrero/hadoop-utilities/raw/main/resources/modified-spark-examples_2.12-3.3.0.jar
# MAGIC wget https://github.com/ronguerrero/HiveUDF/raw/main/target/hive-udf-1.0-SNAPSHOT.jar

# COMMAND ----------

dbutils.fs.cp("file:/tmp/original-spark-examples_2.12-3.3.0.jar", dbfs_resources_path)
dbutils.fs.cp("file:/tmp/modified-spark-examples_2.12-3.3.0.jar", dbfs_resources_path)
dbutils.fs.cp("file:/tmp/hive-udf-1.0-SNAPSHOT.jar", dbfs_resources_path)




# COMMAND ----------

dbutils.fs.ls(dbfs_resources_path)

# COMMAND ----------


