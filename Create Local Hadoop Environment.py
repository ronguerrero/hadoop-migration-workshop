# Databricks notebook source
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
# MAGIC wget https://github.com/ronguerrero/hadoop-utilities/raw/main/raw_transactions2.tgz
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


