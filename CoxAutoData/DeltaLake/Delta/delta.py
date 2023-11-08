from typing import Callable, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from functools import wraps, reduce
from CoxAutoData.DeltaLake.DLT import tableFactory
#todo: passing dlt.tablefactory to execute the package by using
#      sparksession and mocking dlt(DLT.table(name=tableName)(executor))
#      we need a decorator expecting name=tablename then save the df to a location
#      the instaiation is going to suggest save or save as table

def table( **kwags: Any) -> Callable[...,Any]:
    """ store delta table for CoxPyDelta"""
    # name: str,
    # comment: Optional[str] =None,
    # spark_conf: Optional[Dict[str,str]]=None,
    # path: Optional[str]=None,
    # partition_cols: Optional[list]=None,
    # schema: Optional[str]=None,
    # file_format: str = 'delta',
    # todo:
    # table_properties: Optional[Dict[str,str]]=None,
    # temporary: bool =False,
    table_conf = kwags
    def save_table(func: Callable[...,DataFrame] ) -> Callable[...,Any]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            spark = SparkSession.getActiveSession()
            table_name = table_conf.get('name', func.__name__)
            df_res = func(*args, **kwargs)
            spark_conf = list(spark.conf.set(key, value) for key, value in table_conf.get('spark_conf',{}).items())
            write_mode = table_conf.get('mode', "overwrite")
            DataJob = table_conf.get('DataJob', True)
            if DataJob:
                path = table_conf.get('path', None)
                df_writer = df_res.write.mode(write_mode)
                df_writer = df_writer.option('path', path) if path else df_writer
                file_format = table_conf.get('file_format', 'delta')
                if table_conf.get('schema', False):
                    df_writer = df_writer.format(file_format).saveAsTable
                    table_name = f"{table_conf.get('schema')}.{table_name}"
                else:
                    df_writer = df_writer.format(file_format).save

                df_writer(table_name)
        return wrapper()
    return save_table


class CoxDelta:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.table_name = ''
        self.format = 'delta'

    def table(self, name: str) -> Callable[...,Any]:
        """ todo """
        self.table_name = name
        return self.save_table

    def save_table(self, df: DataFrame) -> None:
        """ todo """
        df.write.mode("overwrite").format(self.format).saveAsTable(self.table_name)

