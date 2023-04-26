// Databricks notebook source
// MAGIC %md
// MAGIC ### Scala Script -  Migration
// MAGIC 
// MAGIC #### Objective
// MAGIC Learn how to use Databricks notebooks as alternative to spark-shell
// MAGIC 
// MAGIC #### Technologies Used
// MAGIC ##### Hadoop
// MAGIC * None - leveraging existing Scala JAR code artifact
// MAGIC ##### Databricks
// MAGIC * Spark
// MAGIC 
// MAGIC   
// MAGIC #### Steps
// MAGIC * Review existing Scala Spark code
// MAGIC * Identify required modifications
// MAGIC * Run converted code
// MAGIC 
// MAGIC 
// MAGIC #### Migration Considerations
// MAGIC * Use of Databricks notebooks helps foster live collaboration
// MAGIC * Common code can be bundled into JAR, scala notebooks can contain high level logic.
// MAGIC * Remove any references to Hadoop environment
// MAGIC * Databricks uses Spark 3 - most common change is timestamp format,  but full listing is here - https://spark.apache.org/docs/latest/sql-migration-guide.html#upgrading-from-spark-sql-24-to-30  
// MAGIC   ``` Symbols of ‘E’, ‘F’, ‘q’ and ‘Q’ can only be used for datetime formatting, e.g. date_format. They are not allowed used for datetime parsing, e.g. to_timestamp.```   
// MAGIC   ``` Set spark.sql.legacy.timeParserPolicy to LEGACY where appropriate```

// COMMAND ----------

dbutils.widgets.removeAll()

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


