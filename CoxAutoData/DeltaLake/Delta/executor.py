
import logging
from typing import Any
from pyspark.sql import SparkSession
from CoxAutoData.DeltaLake.DLT import tableFactory
from CoxAutoData.DeltaLake.utils.func import archive_files, getSparkCont
from . import delta

def executor(*args: Any, **kwargs: Any) -> None:
    """ cox delta lake executor"""
    table_list = kwargs.get('tables',None)
    package_list = kwargs.get('packages',[])
    flowLayer = kwargs.get('flow')
    if table_list:
        logging.info(f"executing: table list : {table_list}, modules: {package_list}, layer: {flowLayer} ")
        list(map(tableFactory.importModule,package_list))
        flow = tableFactory.importModule('CoxFlowDLT.flow')
        table_list = getattr(flow, table_list)
        spark = SparkSession.builder.getOrCreate()
        setDatabase(spark)
        delta_executor = tableFactory.deltaTables(spark, delta)
        for i in table_list:
            archive = i.pop('archive',False)
            runFlowLayer = i.pop('flowLayer',flowLayer)
            # logging.info(f"executing: table  : {i.get("tableName")} , modules: {i.get("transform")}, layer: {runFlowLayer} ")
            delta_executor.runFlow(i,runFlowLayer)
            if archive:
                archive_files(**archive)

def setDatabase(spark: SparkSession) -> None:
    catalog = getSparkCont("catalog", sparkSess=spark)
    spConf = []
    if catalog:
        spark.sql(f"use catalog {catalog}")
        spConf.append(catalog)
    database = getSparkCont("database", sparkSess=spark)
    if database:
        spark.sql(f"use schema {database}")
        spConf.append(database)
    if len(spConf)>0:
        spark.conf.set("database",".".join(spConf))