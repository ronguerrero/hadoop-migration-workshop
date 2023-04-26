# Databricks notebook source
# MAGIC %md
# MAGIC ### Pyspark Script - Submit Workload
# MAGIC 
# MAGIC #### Objective
# MAGIC Learn what changes are required for PySpark script migration
# MAGIC 
# MAGIC #### Technologies Used
# MAGIC ##### Hadoop
# MAGIC * None - leveraging existing PySpark code which may by a .py file
# MAGIC ##### Databricks
# MAGIC * Spark
# MAGIC 
# MAGIC #### Steps
# MAGIC * Generate workflow task that will execute a Python script
# MAGIC * The  python script that will be executed can be found here: https://github.com/ronguerrero/hadoop-migration-workshop/blob/main/PySparkPi.py  
# MAGIC 
# MAGIC 
# MAGIC #### NOTES:
# MAGIC This notebook will only generate a workflow job called "Submit Python Script - PySparkPi".  
# MAGIC It will not execute the workflow.  
# MAGIC 
# MAGIC It is also possible to submit python wheel jobs.

# COMMAND ----------

# DBTITLE 1,Environment setup to automatically generate workflow task
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
jar_location = f"dbfs:/tmp/{username}/resources/original-spark-examples_2.12-3.3.0.jar"
modified_jar_location = f"dbfs:/tmp/{username}/resources/modified-spark-examples_2.12-3.3.0.jar"

print(jar_location)
print(f'["--class","org.apache.spark.examples.SparkPi","{jar_location}","10"]')
from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)


url = "https://" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get() 
access_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
clusterId = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

# COMMAND ----------

# DBTITLE 1,JSON string for workflow task definition
workflow_json = """
{   
        "name": "Submit Python Script - PySparkPi",
        "email_notifications": {
            "no_alert_for_skipped_runs": false
        },
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
                     {
                "task_key": "PySpark_Submit",
                "spark_python_task": {
                    "python_file": "PySparkPi.py",
                    "parameters": [
                        "10"
                    ],
                    "source": "GIT"
                },
                "existing_cluster_id": \"""" + clusterId + """\",
                "timeout_seconds": 0,
                "email_notifications": {},
                "notification_settings": {
                    "no_alert_for_skipped_runs": false,
                    "no_alert_for_canceled_runs": false,
                    "alert_on_last_attempt": false
                }
            }
        ],
        "git_source": {
            "git_url": "https://github.com/ronguerrero/hadoop-migration-workshop/",
            "git_provider": "gitHub",
            "git_branch": "main"
        },
        "format": "SINGLE_TASK"
    }
    """  

# COMMAND ----------

# DBTITLE 1,Generate Workflow task based on JSON definition
import requests
my_headers = {"Authorization": "Bearer " + access_token, 'Content-type': 'application/x-www-form-urlencoded'}
response = requests.post(url=url + '/api/2.1/jobs/create', headers=my_headers, data=workflow_json)
response.text
