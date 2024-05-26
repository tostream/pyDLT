from typing import Callable, Any
from functools import wraps
from pyspark.sql import SparkSession, DataFrame
from DeltaPySpark.DeltaLake.Delta.deltaTable import default_conf
from DeltaPySpark.DeltaLake.utils.func import get_spark_conf, praseArg, set_spark_conf,parse_default_arg

#todo: passing dlt.tablefactory to execute the package by using
#      sparksession and mocking dlt(DLT.table(name=tableName)(executor))
#      we need a decorator expecting name=tablename then save the df to a location
#      the instaiation is going to suggest save or save as table
def read(*arg: Any, **kwags: Any) -> Callable[...,Any]:
    spark = SparkSession.getActiveSession()
    return spark.table(*arg,**kwags)

def table( **kwags: Any) -> Callable[...,Any]:
    """ store delta table for PyDelta"""
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
        def wrapper(*args: Any, **kwargs: Any) -> None:
            spark = SparkSession.getActiveSession()
            table_name = table_conf.pop('name', func.__name__)
            set_spark_conf(table_conf.pop('spark_conf',{}))
            # list(spark.conf.set(key, value) for key, value in table_conf.pop('spark_conf',{}).items())
            temporary = table_conf.pop('temporary', False)
            database = table_conf.pop('schema', False)
            if not database:
                database = table_conf.pop('database', False)
            if not database:
                database = get_spark_conf('schema',sparkSess=spark)
            df_res: DataFrame = func(*args, **kwargs)
            if temporary:
                df_res.createOrReplaceTempView(table_name)
            else:
                df_writer = parse_default_arg(df_res.write,default_conf)
                df_writer = praseArg(df_writer,table_conf)
                if database:
                    df_writer = df_writer.saveAsTable
                    table_name = f"{database}.{table_name}"
                else:
                    df_writer = df_writer.save
                df_writer(table_name)
        return wrapper()
    return save_table

