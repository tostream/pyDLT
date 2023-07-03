from typing import Any
from pyspark.sql import SparkSession
from CoxAutoData.DeltaLake.DLT import tableFactory
from . import delta

def executor(*args: Any, **kwargs: Any) -> None:
    """ cox delta lake executor"""
    table_list = kwargs.get('table-list',None)
    package_list = kwargs.get('packages',[])
    if table_list:
        list(map(tableFactory.importModule,package_list))
        flow = tableFactory.importModule('CoxFlowDLT.flow')
        table_list = getattr(flow, table_list)
        spark = SparkSession.builder.getOrCreate()
        delta_executor = tableFactory.deltaTables(spark, delta)
        for i in table_list:
            delta_executor.getRawTables(i)