# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Flag Parameter

# COMMAND ----------

dbutils.widgets.text('incremental_flag', '0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')
print(incremental_flag)

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Dimension Model

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch Relative Column

# COMMAND ----------

df_src = spark.sql('''
select distinct(Model_ID) as Model_ID, Model_Category from parquet.`abfss://silver@cpcldstgdl.dfs.core.windows.net/carsales`
''')

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_model Sink - Intial and Incremental

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_model'):

    df_sink = spark.sql('''
    select Dim_Model_Key, Model_ID, Model_Category from cars_catalog.gold.dim_model
    ''')

else:
    df_sink = spark.sql('''
    select 1 as Dim_Model_Key, Model_ID, Model_Category from parquet.`abfss://silver@cpcldstgdl.dfs.core.windows.net/carsales`
    where 1=0
    ''')

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Filtering new and old records

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src['Model_ID'] == df_sink['Model_ID'], 'left').select(df_src['Model_ID'], df_src['Model_Category'], df_sink['Dim_Model_Key'])
df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # df_filter_old

# COMMAND ----------

df_filter_old = df_filter.filter(col('Dim_Model_Key').isNotNull())
df_filter_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # df_filter_new

# COMMAND ----------

df_filter_new = df_filter.filter(col('Dim_Model_Key').isNull()).select('Model_ID', 'Model_Category')
df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Surrogate Key

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Fetch the max surrogate key from existing table

# COMMAND ----------

if incremental_flag == '0':
    max_value = 1
else:
    max_value_df = spark.sql('select max(Dim_Model_Key) from cars_catalog.gold.dim_model')
    max_value = max_value_df.collect()[0][0]+1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Surrogate key column and ADD the max surrogate key

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('Dim_Model_Key', max_value + monotonically_increasing_id())


# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Final DF - df_filter_old + df_filter_new

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD TYPE - 1 (UPSERT)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

#Incremental Run
if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
  delta_tbl = DeltaTable.forPath(spark, 'abfss://gold@cpcldstgdl.dfs.core.windows.net/dim_model')
  
  delta_tbl.alias('trg').merge(df_final.alias('src'), 'trg.Dim_Model_Key = src.Dim_Model_Key')\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()


#Initial Run
else:
  df_final.write.format('delta')\
    .mode('overwrite')\
    .option('path', 'abfss://gold@cpcldstgdl.dfs.core.windows.net/dim_model')\
    .saveAsTable('cars_catalog.gold.dim_model')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_model;

# COMMAND ----------

