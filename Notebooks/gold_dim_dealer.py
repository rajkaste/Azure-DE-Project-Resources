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
# MAGIC # Creating Dimension Dealer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch Relative Column

# COMMAND ----------

df_src = spark.sql('''
select distinct(Dealer_ID) as Dealer_ID, DealerName from parquet.`abfss://silver@cpcldstgdl.dfs.core.windows.net/carsales`
''')

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_dealer Sink - Intial and Incremental

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):

    df_sink = spark.sql('''
    select Dim_Dealer_Key, Dealer_ID, DealerName from cars_catalog.gold.dim_dealer
    ''')

else:
    df_sink = spark.sql('''
    select 1 as Dim_Dealer_Key, Dealer_ID, DealerName from parquet.`abfss://silver@cpcldstgdl.dfs.core.windows.net/carsales`
    where 1=0
    ''')

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Filtering new and old records

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src['Dealer_ID'] == df_sink['Dealer_ID'], 'left').select(df_src['Dealer_ID'], df_src['DealerName'], df_sink['Dim_Dealer_Key'])
df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # df_filter_old

# COMMAND ----------

df_filter_old = df_filter.filter(col('Dim_Dealer_Key').isNotNull())
df_filter_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # df_filter_new

# COMMAND ----------

df_filter_new = df_filter.filter(col('Dim_Dealer_Key').isNull()).select('Dealer_ID', 'DealerName')
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
    max_value_df = spark.sql('select max(Dim_Dealer_Key) from cars_catalog.gold.dim_dealer')
    max_value = max_value_df.collect()[0][0]+1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Surrogate key column and ADD the max surrogate key

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('Dim_Dealer_Key', max_value + monotonically_increasing_id())


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
if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
  delta_tbl = DeltaTable.forPath(spark, 'abfss://gold@cpcldstgdl.dfs.core.windows.net/dim_dealer')
  
  delta_tbl.alias('trg').merge(df_final.alias('src'), 'trg.Dim_Dealer_Key = src.Dim_Dealer_Key')\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()


#Initial Run
else:
  df_final.write.format('delta')\
    .mode('overwrite')\
    .option('path', 'abfss://gold@cpcldstgdl.dfs.core.windows.net/dim_dealer')\
    .saveAsTable('cars_catalog.gold.dim_dealer')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_dealer;

# COMMAND ----------

