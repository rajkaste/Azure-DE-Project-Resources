# Databricks notebook source
# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

df = spark.read.format('parquet')\
            .option('inferSchema', True)\
            .load('abfss://bronze@cpcldstgdl.dfs.core.windows.net/raw_data')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformation

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = df.withColumn('Model_Category', split(col('Model_ID'),'-')[0])

# COMMAND ----------

df = df.withColumn('RevPerUnit', col('Revenue')/col('Units_Sold'))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # AD-HOC

# COMMAND ----------

df.groupBy('Year','BranchName').agg(sum('Units_Sold').alias('Total_Units')).sort('Year','Total_Units', ascending=[True, False])

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Writing

# COMMAND ----------

df.write.format('parquet')\
        .mode('overwrite')\
        .option('path','abfss://silver@cpcldstgdl.dfs.core.windows.net/carsales')\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # Querying Silver Data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@cpcldstgdl.dfs.core.windows.net/carsales`

# COMMAND ----------

