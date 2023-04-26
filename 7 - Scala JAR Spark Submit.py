# Databricks notebook source
# MAGIC %md
# MAGIC # Using existing Spark JARs in Databricks   
# MAGIC  - External IDEs can be used to build Spark JAR files
# MAGIC  - Remove any references to Hadoop environment
# MAGIC  - Databricks uses Spark 3 - timestamp format changes
# MAGIC  - Job submission will be modified

# COMMAND ----------

username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
jar_location = f"dbfs:/tmp/{username}/resources/original-spark-examples_2.12-3.3.0.jar"
modified_jar_location = f"dbfs:/tmp/{username}/resources/modified-spark-examples_2.12-3.3.0.jar"

print(jar_location)
print(f'["--class","org.apache.spark.examples.SparkPi","{jar_location}","10"]')

# COMMAND ----------

from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)


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

# COMMAND ----------

response.text

# COMMAND ----------

spark_jar_workflow_json = """
{   
        "name": "Spark JAR Job - Spark Pi",
        "email_notifications": {
            "no_alert_for_skipped_runs": false
        },
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "Spark_Jar_SparkPI",
                "spark_jar_task": {
                    "main_class_name" : "org.apache.spark.examples.SparkPi_modified",
                    "parameters": [
                        "10"
                    ]
                },
                "existing_cluster_id":  \"""" + clusterId + """\",
                "libraries": [
                {
                    "jar": \"""" + modified_jar_location + """\"
                }
                ],
                "timeout_seconds": 0,
                "email_notifications": {}
            }
        ],
        "format": "SINGLE_TASK"
    }
    """

# COMMAND ----------

import requests
my_headers = {"Authorization": "Bearer " + access_token, 'Content-type': 'application/x-www-form-urlencoded'}
response = requests.post(url=url + '/api/2.1/jobs/create', headers=my_headers, data=spark_jar_workflow_json)
response.text

# COMMAND ----------


