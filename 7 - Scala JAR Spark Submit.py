# Databricks notebook source
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
jar_location = f"dbfs:/tmp/{username}/resources/original-spark-examples_2.12-3.3.0.jar"

# COMMAND ----------

print(jar_location)

# COMMAND ----------

print(f'["--class","org.apache.spark.examples.SparkPi","{jar_location}","10"]')

# COMMAND ----------


