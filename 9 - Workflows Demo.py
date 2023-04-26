# Databricks notebook source
# MAGIC %md
# MAGIC ### Databricks Worfklows
# MAGIC 
# MAGIC #### Objective
# MAGIC Learn how to create a Databricks workflow.
# MAGIC 
# MAGIC #### Technologies Used
# MAGIC ##### Hadoop
# MAGIC * None - leveraging existing Scala JAR code artifact
# MAGIC ##### Databricks
# MAGIC * Spark
# MAGIC 
# MAGIC   
# MAGIC #### Steps
# MAGIC * Auto-generate workflow comprised of Hive Specific labs
# MAGIC * Walkthrough the workflow definition
# MAGIC * Execute the workflow, and monitor it's progress
# MAGIC 
# MAGIC 
# MAGIC #### Migration Considerations
# MAGIC * Workflows supports
# MAGIC   * forking and merging in DAGs  
# MAGIC   * re-use cluster across workflow tasks  
# MAGIC   * passing parameters across tasks
# MAGIC   * can be triggered on the availability of files on cloud storage
# MAGIC   * can specify a timeout setting - job fails if completion time exceeds limit

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.dbutils import DBUtils
# MAGIC dbutils = DBUtils(spark)
# MAGIC 
# MAGIC username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
# MAGIC jar_location = f"dbfs:/tmp/{username}/resources/original-spark-examples_2.12-3.3.0.jar"
# MAGIC modified_jar_location = f"dbfs:/tmp/{username}/resources/modified-spark-examples_2.12-3.3.0.jar"
# MAGIC 
# MAGIC url = "https://" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get() 
# MAGIC access_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
# MAGIC clusterId = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

# COMMAND ----------

# DBTITLE 1,We'll be calling the Databricks REST APIs, assemble the JSON input 
workflow_json="""
{
    "name": "Run all Hive Migration",
        "email_notifications": {
            "no_alert_for_skipped_runs": false
        },
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "Initialize",
                "notebook_task": {
                    "notebook_path": "1 - Initialization",
                    "source": "GIT"
                },
                "existing_cluster_id":  \"""" + clusterId + """\",
                "timeout_seconds": 0,
                "email_notifications": {}
            },
            {
                "task_key": "Ingestion",
                "depends_on": [
                    {
                        "task_key": "Initialize"
                    }
                ],
                "notebook_task": {
                    "notebook_path": "2 - Data Ingestion",
                    "source": "GIT"
                },
                "existing_cluster_id":  \"""" + clusterId + """\",
                "timeout_seconds": 0,
                "email_notifications": {}
            },
            {
                "task_key": "DDL_Conversion",
                "depends_on": [
                    {
                        "task_key": "Ingestion"
                    }
                ],
                "notebook_task": {
                    "notebook_path": "3 - Hive DDL Conversion",
                    "source": "GIT"
                },
                "existing_cluster_id":  \"""" + clusterId + """\",
                "timeout_seconds": 0,
                "email_notifications": {}
            },
            {
                "task_key": "DML_Conversion",
                "depends_on": [
                    {
                        "task_key": "DDL_Conversion"
                    }
                ],
                "notebook_task": {
                    "notebook_path": "4 - Hive DML Conversion",
                    "source": "GIT"
                },
                "existing_cluster_id":  \"""" + clusterId + """\",
                "timeout_seconds": 0,
                "email_notifications": {}
            },
            {
                "task_key": "UDF_Conversion",
                "depends_on": [
                    {
                        "task_key": "DML_Conversion"
                    },
                    {
                        "task_key": "DDL_Conversion"
                    }
                ],
                "notebook_task": {
                    "notebook_path": "5 - HiveUDFs in Databricks",
                    "source": "GIT"
                },
                "existing_cluster_id":  \"""" + clusterId + """\",
                "timeout_seconds": 0,
                "email_notifications": {}
            } 
        ],
        "git_source": {
            "git_url": "https://github.com/ronguerrero/hadoop-utilities/",
            "git_provider": "gitHub",
            "git_branch": "main"
        },
        "format": "MULTI_TASK"

}
"""

# COMMAND ----------

# DBTITLE 1,Issue the REST call
import requests
my_headers = {"Authorization": "Bearer " + access_token, 'Content-type': 'application/x-www-form-urlencoded'}
response = requests.post(url=url + '/api/2.1/jobs/create', headers=my_headers, data=workflow_json)
response.text

# COMMAND ----------


