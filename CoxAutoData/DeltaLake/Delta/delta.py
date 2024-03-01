from typing import Callable, Any, TypeVar
from pyspark.sql import SparkSession, DataFrame, DataFrameWriter
from functools import wraps
# from CoxAutoData.DeltaLake.DLT import tableFactory
from CoxAutoData.DeltaLake.utils.func import getSparkCont, setSparkConfs
#todo: passing dlt.tablefactory to execute the package by using
#      sparksession and mocking dlt(DLT.table(name=tableName)(executor))
#      we need a decorator expecting name=tablename then save the df to a location
#      the instaiation is going to suggest save or save as table

default_conf = [
    [["coxPyDelta.delta.mergeSchema",""], lambda val: {"option":["mergeSchema", val]}, True ],
    [["coxPyDelta.delta.writeMode",""], lambda val: {"mode": val}, "overwrite"],
]

T = TypeVar('T', bound=DataFrame|DataFrameWriter)
# minic dlt.read
# "Use dlt.read() or spark.table() to perform a complete read from a dataset defined in the same pipeline."
def read(*arg: Any, **kwags: Any) -> T:
    spark = SparkSession.getActiveSession()
    return spark.table(*arg,**kwags)


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
    def save_table(func: Callable[...,DataFrame|DataFrameWriter] ) -> None:
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
            setSparkConfs(table_conf.pop('spark_conf',{}))
            temporary = table_conf.pop('temporary', False)
            database = table_conf.pop('database', False)
            if not database:
                database = getSparkCont('database',sparkSess=spark)
            df_res: DataFrame = func(*args, **kwargs)
            if temporary:
                df_res.createOrReplaceTempView(table_name)
            else:
                df_writer = parseDefaultArg(df_res.write)
                df_writer = praseArg(df_writer,table_conf)
                if database:
                    df_writer = df_writer.saveAsTable
                    table_name = f"{database}.{table_name}"
                else:
                    df_writer = df_writer.save
                df_writer(table_name)
        return wrapper()
    return save_table



def parseDefaultArg(func: T) -> T:
    for conf in default_conf:
        val = getSparkCont(*conf[0])
        if not val: val = conf[2]
        argFunc = conf[1]
        func = praseArg(func,argFunc(val))
    return func


def praseArg(func: T, conf: dict)->T:
    for k,v in conf.items():
        if type(v) == dict:
            func = addAction(func,k,**v)
        elif type(v) == list:
            func = addAction(func,k,*v)
        else:
            func = addAction(func,k,v)
    return func

def addAction(func: T, option:str,*arg:Any,**kwargg:Any) -> T:
    func = getattr(func,option)
    return func(*arg,**kwargg)


