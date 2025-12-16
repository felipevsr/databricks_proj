# Databricks notebook source
# MAGIC %md
# MAGIC ## CATALOG

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS databricks_proj

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATABASES

# COMMAND ----------

databases = ["bronze","silver","gold"]

for db in databases:
  spark.sql(f""" CREATE DATABASE IF NOT EXISTS databricks_proj.{db} """)
  print(f"Database databricks_proj.{db} created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volumes 

# COMMAND ----------

volumes_worspace = ["raw_json","bronze","silver","gold","historic"]
for itens in volumes_worspace:
  spark.sql(f""" CREATE VOLUME IF NOT EXISTS workspace.default.{itens} """)
  print(f"Volume d workspace.default.{itens} created")


# COMMAND ----------

# %sql
# DROP VOLUME workspace.default.silver

# COMMAND ----------

# df = spark.read.format('csv').option("header",True).load('dbfs:/databricks-datasets/flights/departuredelays.csv')
# df.write.mode("overwrite").format('delta').save('/Volumes/workspace/default/teste100')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE TEMPO_DIST
# MAGIC USING DELTA
# MAGIC AS
# MAGIC select * from delta.`dbfs:/Volumes/workspace/default/teste100/`

# COMMAND ----------

spark.sql(""" CREATE TABLE IF NOT EXISTS  databricks_proj.bronze.teste2000   
          USING DELTA 
          AS
          select * from delta.`/Volumes/workspace/default/teste100/` """)

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table databricks_proj.bronze.teste2000   

# COMMAND ----------

# MAGIC %sql
# MAGIC select origin,sum(distance) 
# MAGIC  from databricks_proj.bronze.teste2000
# MAGIC  group by all

# COMMAND ----------

df = spark.read.format('csv').option("header",True).load('/FileStore/tables/departuredelays.csv')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.FireData

# COMMAND ----------

df.write.mode("overwrite").format('delta').save('/Volumes/workspace/default/firedata')

# COMMAND ----------

## DA ERRO
# %sql
# create table if not exists databricks_proj.bronze.born1000  using delta location '/Volumes/workspace/default/teste100/'
