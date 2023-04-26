-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####This notebook will demonstrate how to register existing Hive UDFs into Databricks
-- MAGIC 
-- MAGIC The UDF will be using can be found in this git repo: https://github.com/ronguerrero/HiveUDF   
-- MAGIC It's based on this UDF example from here: https://github.com/ronguerrero/HiveUDF  
-- MAGIC For the purposes of this demo, we'll use the pre-compliled JAR file: https://github.com/ronguerrero/HiveUDF/blob/main/target/hive-udf-1.0-SNAPSHOT.jar   
-- MAGIC 
-- MAGIC 
-- MAGIC Please download hive-udf-1.0-SNAPSHOT.jar to your local computer

-- COMMAND ----------

-- MAGIC %md
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


