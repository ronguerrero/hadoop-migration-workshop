// Databricks notebook source
// MAGIC %python
// MAGIC dbutils.widgets.removeAll()

// COMMAND ----------

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples

import scala.math.random

import org.apache.spark.sql.SparkSession

/** Computes an approximation to pi */
object SparkPi {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .getOrCreate()
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y <= 1) 1 else 0
    }.reduce(_ + _)
    println(s"Pi is roughly ${4.0 * count / (n - 1)}")
    spark.stop()
  }
}
// scalastyle:on println

// COMMAND ----------

dbutils.widgets.removeAll
dbutils.widgets.text("slices", "10", "Slices")

// COMMAND ----------

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
// package org.apache.spark.examples

import scala.math.random

import org.apache.spark.sql.SparkSession

/** Computes an approximation to pi */
// object SparkPi {
//   def main(args: Array[String]): Unit = {
//    val spark = SparkSession
//      .builder
//      .appName("Spark Pi")
//      .getOrCreate()

//    val slices = if (args.length > 0) args(0).toInt else 2
    val slices = dbutils.widgets.get("slices").toInt
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y <= 1) 1 else 0
    }.reduce(_ + _)
    println(s"Pi is roughly ${4.0 * count / (n - 1)}")
//    spark.stop()
//  }
//}
// scalastyle:on println

// COMMAND ----------

// MAGIC %python
// MAGIC username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
// MAGIC jar_location = f"dbfs:/tmp/{username}/resources/original-spark-examples_2.12-3.3.0.jar"
// MAGIC modified_jar_location = f"dbfs:/tmp/{username}/resources/modified-spark-examples_2.12-3.3.0.jar"

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.dbutils import DBUtils
// MAGIC dbutils = DBUtils(spark)
// MAGIC 
// MAGIC 
// MAGIC url = "https://" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get() 
// MAGIC access_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
// MAGIC clusterId = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

// COMMAND ----------

// MAGIC %python
// MAGIC spark_jar_workflow_json = """
// MAGIC {   
// MAGIC         "name": "Spark JAR Job - Spark Pi",
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
