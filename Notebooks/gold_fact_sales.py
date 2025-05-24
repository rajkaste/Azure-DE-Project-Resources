# Databricks notebook source
# MAGIC %md
# MAGIC # Create Fact Table

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading Silver Data**

# COMMAND ----------


df_silver = spark.sql('''
select * from parquet.`abfss://silver@cpcldstgdl.dfs.core.windows.net/carsales`
''')

# COMMAND ----------

df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading all Dims

# COMMAND ----------

df_model = spark.sql('''select * from cars_catalog.gold.dim_model''')

df_branch = spark.sql('''select * from cars_catalog.gold.dim_branch''')

df_dealer = spark.sql('''select * from cars_catalog.gold.dim_dealer''')


# COMMAND ----------

# MAGIC %md
# MAGIC # Bringing keys to the Fact Table

# COMMAND ----------

df_fact = df_silver.join(df_branch, df_silver['Branch_ID'] == df_branch['Branch_ID'], how='left')\
                    .join(df_model, df_silver['Model_ID'] == df_model['Model_ID'], how='left')\
                    .join(df_dealer, df_silver['Dealer_ID'] == df_dealer['Dealer_ID'], how='left')\
                    .select(df_silver['Revenue'], df_silver['Units_Sold'], df_silver['RevPerUnit'], df_model['dim_model_key'], df_branch['dim_branch_key'], df_dealer['dim_dealer_key'])


# COMMAND ----------

df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing Fact Table

# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists('factsales'):
    deltatbl = DeltaTable.forName(spark, 'cars_catalog.gold.factsales')
    deltatbl.alias('trg').merge(df_fact.alias('src'), 'trg.dim_model_key = src.dim_model_key and trg.dim_branch_key = src.dim_branch_key and trg.dim_dealer_key = src.dim_dealer_key')\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()

else:
    df_fact.write.format('delta')\
                .mode('overwrite')\
                .option('path', 'abfss://gold@cpcldstgdl.dfs.core.windows.net/factsales')\
                .saveAsTable('cars_catalog.gold.factsales')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.factsales;

# COMMAND ----------

