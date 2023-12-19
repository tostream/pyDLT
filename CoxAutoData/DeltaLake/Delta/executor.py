from typing import Any
from pyspark.sql import SparkSession
from CoxAutoData.DeltaLake.DLT import tableFactory
from CoxAutoData.DeltaLake.utils.func import archive_files
from . import delta

def executor(*args: Any, **kwargs: Any) -> None:
    """ cox delta lake executor"""
    table_list = kwargs.get('tables',None)
    package_list = kwargs.get('packages',[])
    if table_list:
        list(map(tableFactory.importModule,package_list))
        flow = tableFactory.importModule('CoxFlowDLT.flow')
        table_list = getattr(flow, table_list)
        spark = SparkSession.builder.getOrCreate()
        delta_executor = tableFactory.deltaTables(spark, delta)
        for i in table_list:
            archive = i.pop('archive',False)
            delta_executor.getRawTables(i)
            if archive:
                archive_files(**archive)
            