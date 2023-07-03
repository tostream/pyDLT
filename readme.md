CoxPyDelta

Cox Automotive Delta lake python framework.
To replace Scala Data Lake framework waimak. We are going to use Databricks Delta Live Table to execute the transformation.
This Package is going to provide a few extra feature to support us implement data pipeline in Python.


1. CoxDLT_v2.py is a notebook to execute CoxPyDelta pipeline in Databricks Delta Live table workflow
2. Delta executor is a moudule to execute CoxPyDelta pipeline in Databricks Spark job and custom Python module
3. Utils is a module with common function.

*There is a CookieCutter repo (https://ghe.coxautoinc.com/DataServices/DLT_Cookiecutter) to generate the folder sturcture of a CoxPyDelta pipeline.