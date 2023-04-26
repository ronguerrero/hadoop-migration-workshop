-- Databricks notebook source
-- MAGIC %md
-- MAGIC This notebook is used to add the Hive UDF to your cluster if you are having issues adding it manually.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
-- MAGIC dbfs_resources_path = f"/tmp/{username}/resources/"
-- MAGIC os_dbfs_resources_path = f"/dbfs/" + dbfs_resources_path
-- MAGIC spark.conf.set("c.database", username)
-- MAGIC database=username
-- MAGIC 
-- MAGIC import os
-- MAGIC os.environ['DBFS_RESOURCES_PATH'] = os_dbfs_resources_path
-- MAGIC os.environ['DATABASE'] = database
-- MAGIC 
-- MAGIC udf_location = f"dbfs:/tmp/{username}/resources/hive-udf-1.0-SNAPSHOT.jar"
-- MAGIC 
-- MAGIC from pyspark.dbutils import DBUtils
-- MAGIC dbutils = DBUtils(spark)
-- MAGIC 
-- MAGIC url = "https://" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get() 
-- MAGIC access_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
-- MAGIC clusterId = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC libraries_json = """
-- MAGIC {
-- MAGIC   "cluster_id": \"""" + clusterId + """\",
-- MAGIC   "libraries": [
-- MAGIC     {
-- MAGIC       "jar": \"""" + udf_location + """\"
-- MAGIC     }
-- MAGIC   ]
-- MAGIC }
-- MAGIC """

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import requests
-- MAGIC my_headers = {"Authorization": "Bearer " + access_token, 'Content-type': 'application/x-www-form-urlencoded'}
-- MAGIC response = requests.post(url=url + '/api/2.0/libraries/install', headers=my_headers, data=libraries_json)
-- MAGIC response.text

-- COMMAND ----------


