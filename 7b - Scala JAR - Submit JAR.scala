// Databricks notebook source
// MAGIC %md
// MAGIC ### Scala Code (JAR) -  Migration
// MAGIC 
// MAGIC #### Objective
// MAGIC Learn how to submit compiled JAR files into Databricks using recommended Submit JAR approach
// MAGIC #### Technologies Used
// MAGIC 
// MAGIC ##### Databricks
// MAGIC * Spark
// MAGIC 
// MAGIC   
// MAGIC #### Steps
// MAGIC * Review existing Scala Spark code
// MAGIC * Identify required modifications
// MAGIC * Auto-generate a Databricks workflow job that submits a pre-compiled JAR
// MAGIC * Run the generated Databricks workflow - "Submit JAR Job - Spark Pi"
// MAGIC 
// MAGIC 
// MAGIC #### Migration Considerations
// MAGIC * Submit JAR is the recommended mechanism to execute a Spark JAR file in Databricks - autoscaling, re-using existing cluster for developement purpose

// COMMAND ----------

// MAGIC %md
// MAGIC ```
// MAGIC /*
// MAGIC  * Licensed to the Apache Software Foundation (ASF) under one or more
// MAGIC  * contributor license agreements.  See the NOTICE file distributed with
// MAGIC  * this work for additional information regarding copyright ownership.
// MAGIC  * The ASF licenses this file to You under the Apache License, Version 2.0
// MAGIC  * (the "License"); you may not use this file except in compliance with
// MAGIC  * the License.  You may obtain a copy of the License at
// MAGIC  *
// MAGIC  *    http://www.apache.org/licenses/LICENSE-2.0
// MAGIC  *
// MAGIC  * Unless required by applicable law or agreed to in writing, software
// MAGIC  * distributed under the License is distributed on an "AS IS" BASIS,
// MAGIC  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// MAGIC  * See the License for the specific language governing permissions and
// MAGIC  * limitations under the License.
// MAGIC  */
// MAGIC 
// MAGIC // scalastyle:off println
// MAGIC package org.apache.spark.examples
// MAGIC 
// MAGIC import scala.math.random
// MAGIC 
// MAGIC import org.apache.spark.sql.SparkSession
// MAGIC 
// MAGIC /** Computes an approximation to pi */
// MAGIC object SparkPi {
// MAGIC   def main(args: Array[String]): Unit = {
// MAGIC     val spark = SparkSession
// MAGIC       .builder
// MAGIC       .appName("Spark Pi")
// MAGIC       .getOrCreate()
// MAGIC     val slices = if (args.length > 0) args(0).toInt else 2
// MAGIC     val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
// MAGIC     val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
// MAGIC       val x = random * 2 - 1
// MAGIC       val y = random * 2 - 1
// MAGIC       if (x*x + y*y <= 1) 1 else 0
// MAGIC     }.reduce(_ + _)
// MAGIC     println(s"Pi is roughly ${4.0 * count / (n - 1)}")
// MAGIC   // DO NOT CALL spark.stop() when using Submit JAR
// MAGIC   //  spark.stop()
// MAGIC   }
// MAGIC }
// MAGIC ```

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.dbutils import DBUtils
// MAGIC dbutils = DBUtils(spark)
// MAGIC 
// MAGIC username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
// MAGIC jar_location = f"dbfs:/tmp/{username}/resources/original-spark-examples_2.12-3.3.0.jar"
// MAGIC modified_jar_location = f"dbfs:/tmp/{username}/resources/modified-spark-examples_2.12-3.3.0.jar"
// MAGIC 
// MAGIC url = "https://" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get() 
// MAGIC access_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
// MAGIC clusterId = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

// COMMAND ----------

// MAGIC %python
// MAGIC spark_jar_workflow_json = """
// MAGIC {   
// MAGIC         "name": "Submit JAR Job - Spark Pi",
// MAGIC         "email_notifications": {
// MAGIC             "no_alert_for_skipped_runs": false
// MAGIC         },
// MAGIC         "webhook_notifications": {},
// MAGIC         "timeout_seconds": 0,
// MAGIC         "max_concurrent_runs": 1,
// MAGIC         "tasks": [
// MAGIC             {
// MAGIC                 "task_key": "Spark_Jar_SparkPI",
// MAGIC                 "spark_jar_task": {
// MAGIC                     "main_class_name" : "org.apache.spark.examples.SparkPi_modified",
// MAGIC                     "parameters": [
// MAGIC                         "10"
// MAGIC                     ]
// MAGIC                 },
// MAGIC                 "existing_cluster_id":  \"""" + clusterId + """\",
// MAGIC                 "libraries": [
// MAGIC                 {
// MAGIC                     "jar": \"""" + modified_jar_location + """\"
// MAGIC                 }
// MAGIC                 ],
// MAGIC                 "timeout_seconds": 0,
// MAGIC                 "email_notifications": {}
// MAGIC             }
// MAGIC         ],
// MAGIC         "format": "SINGLE_TASK"
// MAGIC     }
// MAGIC     """

// COMMAND ----------

// MAGIC %python
// MAGIC import requests
// MAGIC my_headers = {"Authorization": "Bearer " + access_token, 'Content-type': 'application/x-www-form-urlencoded'}
// MAGIC response = requests.post(url=url + '/api/2.1/jobs/create', headers=my_headers, data=spark_jar_workflow_json)
// MAGIC response.text

// COMMAND ----------


