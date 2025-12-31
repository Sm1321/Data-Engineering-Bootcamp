# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC ####1. How to create Spark Session

# COMMAND ----------

from pyspark.sql import SparkSession

spark_session = SparkSession.builder.getOrCreate()

spark_session.version

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. What is pre-created Spark Session

# COMMAND ----------

spark.version

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. How to use SparkSession to read table data?

# COMMAND ----------

df = spark.table("dev.spark_db.diamonds")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>
# MAGIC