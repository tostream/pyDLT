# Databricks notebook source
dataFlowPackagePath = spark.conf.get("execute.lib")
%pip install $dataFlowPackagePath
%pip install CoxAutoData

# COMMAND ----------


from pyspark.sql.functions import col, lit, regexp_replace, when, coalesce, avg, max, sum
import dlt
from CoxAutoData.DeltaLake.DLT.tableFactory import deltaTables
from CoxFlowDLT.flow import _rawTablesList, _silverTablesList, _goldTablesList

# COMMAND ----------

#manage all dependancy lib in this cell
#db_name = 'dev_mandata_business_objects_py_develop'
delta = deltaTables(spark,dlt)
#import mandata_business_object.silver

# COMMAND ----------

# DBTITLE 1,Load Data

rawTablesList = _rawTablesList
for i in rawTablesList:
  delta.getRawTables(i)

# COMMAND ----------

# DBTITLE 1,Ideals

silverTablesList=_silverTablesList
for i in silverTablesList:
  delta.getIdealTables(i)

# COMMAND ----------

# DBTITLE 1,Business Object

goldTablesList=_goldTablesList
for i in goldTablesList:
  delta.getBOTables(i)
