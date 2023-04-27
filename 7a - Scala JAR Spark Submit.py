# Databricks notebook source
# MAGIC %md
# MAGIC ### Scala Code (JAR) -  Migration
# MAGIC 
# MAGIC #### Objective
# MAGIC Learn how to submit compiled JAR files into Databricks using spark-submit semantics.   
# MAGIC NOTE - YOU WILL NOT BE ABLE TO RUN THE WORKFLOW IN THE WEBINAR LAB ENVIRONMENT!   
# MAGIC The generated workflow requires permissions to create clusters.  Do not request this permission in the lab environment.   
# MAGIC The Spark Submit can be scripted using REST API calls i.e. called by 3rd Party schedulers or tools
# MAGIC 
# MAGIC 
# MAGIC #### Technologies Used
# MAGIC 
# MAGIC ##### Databricks
# MAGIC * Spark
# MAGIC 
# MAGIC   
# MAGIC #### Steps
# MAGIC * Review existing Scala Spark code
# MAGIC * Identify required modifications
# MAGIC * Auto-generate a Databricks workflow job that submits a pre-compiled jar - "Spark Submit - Spark Pi"
# MAGIC * NOTE - you will not be able to run this in the webinar environment!
# MAGIC 
# MAGIC 
# MAGIC #### Migration Considerations
# MAGIC * You can use local IDE for development - https://docs.databricks.com/dev-tools/databricks-connect.html
# MAGIC * External IDEs can be used to build Spark JAR files.  JARs can be built within CI/CD processes.
# MAGIC * Typical code changes:
# MAGIC   * Remove any references to Hadoop environment
# MAGIC   * Databricks uses Spark 3 - most common change is timestamp format,  but full listing is here - https://spark.apache.org/docs/latest/sql-migration-guide.html#upgrading-from-spark-sql-24-to-30  
# MAGIC     ``` Symbols of ‘E’, ‘F’, ‘q’ and ‘Q’ can only be used for datetime formatting, e.g. date_format. They are not allowed used for datetime parsing, e.g. to_timestamp.```   
# MAGIC     ``` Set spark.sql.legacy.timeParserPolicy to LEGACY where appropriate```
# MAGIC   * Job submission will be modified - spark-submit semantics are supported, but there are limitations - https://docs.databricks.com/dev-tools/api/2.0/jobs.html#sparksubmittask   
# MAGIC   ```
# MAGIC      * You can invoke Spark submit tasks only on new clusters.
# MAGIC     * In the new_cluster specification, libraries and spark_conf are not supported. Instead, use --jars and --py-files to add Java and Python libraries and --conf to set the Spark configuration.
# MAGIC     * master, deploy-mode, and executor-cores are automatically configured by Databricks; you cannot specify them in parameters.
# MAGIC     * By default, the Spark submit job uses all available memory (excluding reserved memory for Databricks services). You can set --driver-memory, and --executor-memory to a smaller value to leave some room for off-heap usage.
# MAGIC    ```
# MAGIC   * Recommendation - use Submit JAR instead

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC /*
# MAGIC  * Licensed to the Apache Software Foundation (ASF) under one or more
# MAGIC  * contributor license agreements.  See the NOTICE file distributed with
# MAGIC  * this work for additional information regarding copyright ownership.
# MAGIC  * The ASF licenses this file to You under the Apache License, Version 2.0
# MAGIC  * (the "License"); you may not use this file except in compliance with
# MAGIC  * the License.  You may obtain a copy of the License at
# MAGIC  *
# MAGIC  *    http://www.apache.org/licenses/LICENSE-2.0
# MAGIC  *
# MAGIC  * Unless required by applicable law or agreed to in writing, software
# MAGIC  * distributed under the License is distributed on an "AS IS" BASIS,
# MAGIC  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# MAGIC  * See the License for the specific language governing permissions and
# MAGIC  * limitations under the License.
# MAGIC  */
# MAGIC 
# MAGIC // scalastyle:off println
# MAGIC package org.apache.spark.examples
# MAGIC 
# MAGIC import scala.math.random
# MAGIC 
# MAGIC import org.apache.spark.sql.SparkSession
# MAGIC 
# MAGIC /** Computes an approximation to pi */
# MAGIC object SparkPi {
# MAGIC   def main(args: Array[String]): Unit = {
# MAGIC     val spark = SparkSession
# MAGIC       .builder
# MAGIC       .appName("Spark Pi")
# MAGIC       .getOrCreate()
# MAGIC     val slices = if (args.length > 0) args(0).toInt else 2
# MAGIC     val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
# MAGIC     val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
# MAGIC       val x = random * 2 - 1
# MAGIC       val y = random * 2 - 1
# MAGIC       if (x*x + y*y <= 1) 1 else 0
# MAGIC     }.reduce(_ + _)
# MAGIC     println(s"Pi is roughly ${4.0 * count / (n - 1)}")
# MAGIC     spark.stop()
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Set up lab environment specifics
from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)

username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
jar_location = f"dbfs:/tmp/{username}/resources/original-spark-examples_2.12-3.3.0.jar"
modified_jar_location = f"dbfs:/tmp/{username}/resources/modified-spark-examples_2.12-3.3.0.jar"

url = "https://" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get() 
access_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
clusterId = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

# COMMAND ----------

workflow_json = """
{   
        "name": "Spark Submit - Spark Pi",
        "email_notifications": {
            "no_alert_for_skipped_runs": false
        },
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "sparkPi_New",
                "spark_submit_task": {
                    "parameters": [
                        "--class",
                        "org.apache.spark.examples.SparkPi",
                        \"""" + jar_location + """\",
                        "10"
                    ]
                },
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "11.3.x-scala2.12",
                    "spark_conf": {
                        "spark.databricks.delta.preview.enabled": "true",
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode"
                    },
                    "azure_attributes": {
                        "first_on_demand": 1,
                        "availability": "ON_DEMAND_AZURE",
                        "spot_bid_max_price": -1
                    },
                    "node_type_id": "Standard_DS3_v2",
                    "custom_tags": {
                        "ResourceClass": "SingleNode"
                    },
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": true,
                    "data_security_mode": "LEGACY_SINGLE_USER_STANDARD",
                    "runtime_engine": "STANDARD",
                    "num_workers": 0
                },
                "timeout_seconds": 0,
                "email_notifications": {}
            }
        ],
        "format": "MULTI_TASK"
    }
    """

# COMMAND ----------

import requests
my_headers = {"Authorization": "Bearer " + access_token, 'Content-type': 'application/x-www-form-urlencoded'}
response = requests.post(url=url + '/api/2.1/jobs/create', headers=my_headers, data=workflow_json)
response.text

# COMMAND ----------


