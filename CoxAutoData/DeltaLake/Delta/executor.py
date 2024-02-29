from typing import Any
from pyspark.sql import SparkSession
from CoxAutoData.DeltaLake.DLT import tableFactory
from CoxAutoData.DeltaLake.utils.func import archive_files, getSparkCont
from . import delta

def executor(*args: Any, **kwargs: Any) -> None:
    """ cox delta lake executor"""
    table_list = kwargs.get('tables',None)
    package_list = kwargs.get('packages',[])
    flowLayer = kwargs.get('flowLayer')
    if table_list:
        list(map(tableFactory.importModule,package_list))
        flow = tableFactory.importModule('CoxFlowDLT.flow')
        table_list = getattr(flow, table_list)
        spark = SparkSession.builder.getOrCreate()
        setDatabase(spark)
        delta_executor = tableFactory.deltaTables(spark, delta)
        for i in table_list:
            archive = i.pop('archive',False)
            runFlowLayer = i.pop('flowLayer',flowLayer)
            delta_executor.runFlow(i,runFlowLayer)
            if archive:
                archive_files(**archive)

def setDatabase(spark: SparkSession) -> None:
    catalog = getSparkCont("catalog", sparkSess=spark)
    if catalog:
        spark.sql(f"use catalog {catalog}")
    database = getSparkCont("database", sparkSess=spark)
    if database:
        spark.sql(f"use schema {database}")
