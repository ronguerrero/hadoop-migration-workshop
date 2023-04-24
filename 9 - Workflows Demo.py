# Databricks notebook source
from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)


url = "https://" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get() 
access_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
clusterId = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

# COMMAND ----------

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

import requests
my_headers = {"Authorization": "Bearer " + access_token, 'Content-type': 'application/x-www-form-urlencoded'}
response = requests.post(url=url + '/api/2.1/jobs/create', headers=my_headers, data=workflow_json)

# COMMAND ----------

response.text

# COMMAND ----------


