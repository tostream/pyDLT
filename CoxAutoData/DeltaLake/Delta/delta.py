from typing import Callable, Any, Optional, TypeVar, Generic
from pyspark.sql import SparkSession, DataFrame, DataFrameWriter
from functools import wraps, reduce
from CoxAutoData.DeltaLake.DLT import tableFactory
#todo: passing dlt.tablefactory to execute the package by using
#      sparksession and mocking dlt(DLT.table(name=tableName)(executor))
#      we need a decorator expecting name=tablename then save the df to a location
#      the instaiation is going to suggest save or save as table

T = TypeVar('T', bound=DataFrame|DataFrameWriter)

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
    def save_table(func: Callable[...,DataFrame|DataFrameWriter] ) -> Callable[...,Any]:
        """store spark table
 
        Args:
            func (Callable[...,DataFrame|DataFrameWriter]): DataFrameWriter 

        Returns:
            Callable[...,Any]: DataFrame|DataFrameWriter
        """
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> None:
            spark = SparkSession.getActiveSession()
            table_name = table_conf.pop('name', func.__name__)
            list(spark.conf.set(key, value) for key, value in table_conf.pop('spark_conf',{}).items())
            temporary = table_conf.pop('temporary', False)
            database = table_conf.pop('database', False)
            if not database:
                database = getSparkCont('database')
            df_res: DataFrame = func(*args, **kwargs)
            if temporary:
                df_res.createOrReplaceTempView(table_name)
            else:
                df_writer = praseArg(df_res.write,table_conf)
                if database:
                    df_writer = df_writer.saveAsTable
                    table_name = f"{database}.{table_name}"
                else:
                    df_writer = df_writer.save
                df_writer(table_name)
        return wrapper()
    return save_table

def getSparkCont(param) -> Optional[str] :
    spark = SparkSession.getActiveSession()
    return spark.conf.get(param,None)


def praseArg(func: T, conf: dict)->T:
    for k,v in conf.items():
        func = addAction(func,k,*v)
    return func

def addAction(func: T, option:str,*arg:Any,**kwargg:Any) -> T:
    func = getattr(func,option)
    return func(*arg,**kwargg)

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

