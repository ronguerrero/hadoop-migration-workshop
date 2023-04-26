-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Using Hive UDFs in Databricks
-- MAGIC 
-- MAGIC #### Objectives
-- MAGIC Learn about Hive UDF compatibility in Databricks
-- MAGIC 
-- MAGIC #### Technologies Used
-- MAGIC ##### Hadoop
-- MAGIC * Hive - to show original execution of UDF 
-- MAGIC ##### Databricks
-- MAGIC * SQL
-- MAGIC   
-- MAGIC #### Steps
-- MAGIC * Walkthrough the UDF code to explain what it does
-- MAGIC * Demonstrate adding the UDF jar file to the cluster
-- MAGIC * Registering the Hive UDF into Databricks
-- MAGIC * Executing the Hive UDF with SQL in Databricks
-- MAGIC 
-- MAGIC #### Databricks UDF Best Practices
-- MAGIC 
-- MAGIC https://docs.databricks.com/udf/index.html#which-udfs-are-most-efficient  
-- MAGIC Some UDFs are more efficient than others. In terms of performance:
-- MAGIC 
-- MAGIC * Built in functions will be fastest because of Databricks optimizers.  
-- MAGIC * Code that executes in the JVM (Scala, Java, Hive UDFs) will be faster than Python UDFs.  
-- MAGIC * Pandas UDFs use Arrow to reduce serialization costs associated with Python UDFs.  
-- MAGIC * Python UDFs should generally be avoided, but Python can be used as glue code without any degradation in performance.  
-- MAGIC 
-- MAGIC 
-- MAGIC #### Setup
-- MAGIC 
-- MAGIC The UDF will be using can be found in this git repo: https://github.com/ronguerrero/HiveUDF   
-- MAGIC It's based on this UDF example from here: https://github.com/ronguerrero/HiveUDF  
-- MAGIC For the purposes of this demo, we'll use the pre-compliled JAR file: https://github.com/ronguerrero/HiveUDF/blob/main/target/hive-udf-1.0-SNAPSHOT.jar   
-- MAGIC 
-- MAGIC 
-- MAGIC Please download hive-udf-1.0-SNAPSHOT.jar to your local computer.  
-- MAGIC Attach the downloaded library to your Databricks cluster.  
-- MAGIC Follow along in the demo.  Or refer to the following instructions: https://docs.databricks.com/libraries/cluster-libraries.html#workspace-library

-- COMMAND ----------

-- DBTITLE 1,The UDF returns the underlying java type for a column passed in as parameter
-- MAGIC %md
-- MAGIC 
-- MAGIC This is the code:
-- MAGIC 
-- MAGIC ```
-- MAGIC package com.mycompany.hiveudf;
-- MAGIC 
-- MAGIC import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
-- MAGIC import org.apache.hadoop.hive.ql.metadata.HiveException;
-- MAGIC import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
-- MAGIC import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
-- MAGIC import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
-- MAGIC import org.apache.hadoop.io.Text;
-- MAGIC 
-- MAGIC public class TypeOf extends GenericUDF {
-- MAGIC     private final Text output = new Text();
-- MAGIC 
-- MAGIC     @Override
-- MAGIC     public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
-- MAGIC         checkArgsSize(arguments, 1, 1);
-- MAGIC         checkArgPrimitive(arguments, 0);
-- MAGIC         ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
-- MAGIC         return outputOI;
-- MAGIC     }
-- MAGIC 
-- MAGIC     @Override
-- MAGIC     public Object evaluate(DeferredObject[] arguments) throws HiveException {
-- MAGIC         Object obj;
-- MAGIC         if ((obj = arguments[0].get()) == null) {
-- MAGIC             String res = "Type: NULL";
-- MAGIC             output.set(res);
-- MAGIC         } else {
-- MAGIC             String res = "Type: " + obj.getClass().getName();
-- MAGIC             output.set(res);
-- MAGIC         }
-- MAGIC         return output;
-- MAGIC     }
-- MAGIC 
-- MAGIC     @Override
-- MAGIC     public String getDisplayString(String[] children) {
-- MAGIC         return getStandardDisplayString("TYPEOF", children, ",");
-- MAGIC     }
-- MAGIC }
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os
-- MAGIC from pyspark.dbutils import DBUtils
-- MAGIC dbutils = DBUtils(spark)
-- MAGIC 
-- MAGIC username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
-- MAGIC spark.conf.set("c.database", username)

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC export HADOOP_HOME=/usr/local/hadoop/
-- MAGIC export HIVE_HOME=/usr/local/hive/
-- MAGIC export PATH=$HADOOP_HOME/bin:$HIVE_HOME/bin:$PATH
-- MAGIC cd $HIVE_HOME
-- MAGIC 
-- MAGIC # in this environment the Hive UDF resides in the Hive lib directory
-- MAGIC ls -l $HIVE_HOME/lib/hive-udf-1.0-SNAPSHOT.jar
-- MAGIC 
-- MAGIC hive -e "select udftypeof(id) from raw_transactions limit 1"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For more details on how to register Hive UDFs in Databricks, refer to:  https://kb.databricks.com/data/hive-udf.html

-- COMMAND ----------

USE ${c.database};
DROP TEMPORARY FUNCTION IF EXISTS udftypeof;
CREATE TEMPORARY FUNCTION udftypeof AS 'com.mycompany.hiveudf.TypeOf'

-- COMMAND ----------

-- DBTITLE 1,Let's display the java type
SELECT udftypeof(id) FROM raw_transactions limit 1

-- COMMAND ----------

-- DBTITLE 1,Full transparency - Spark SQL already has this built-in function :)
SELECT TYPEOF(id) FROM raw_transactions limit 1

-- COMMAND ----------


