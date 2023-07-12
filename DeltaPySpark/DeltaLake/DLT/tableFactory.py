
from typing import Any, Callable, Dict, Optional

from pyspark.sql import dataframe

from DeltaPySpark.DeltaLake.DLT import loader
from DeltaPySpark.DeltaLake.transforms.transform import deltaLiveTable

""" using plug-in architecture to separated tranformation and pipeline"""

table_creation_funcs: Dict[str, Callable[..., Any]] = {}

def register(table_name: str, creator_fn: Callable[..., Any]) -> None:
    """Register a transformation."""
    table_creation_funcs[table_name] = creator_fn

def createTransform(arguments: Dict[str, Any]) -> deltaLiveTable:
    """get the transformation logic"""

    transform = arguments.pop("transform")
    try:
        creator_func = table_creation_funcs[transform]
    except KeyError:
        raise ValueError(f"unknown character type {transform!r}") from None
    return creator_func(transform)

def createExternalSource(arguments: Dict[str, Any]) -> Callable:
    """get the python package retrieve data by customer python package"""

    transform = arguments.pop("transformName")
    initPara = arguments.pop("instantiation")
    try:
        creator_func = table_creation_funcs[transform]
    except KeyError:
        raise ValueError(f"unknown character type {transform!r}") from None
    return creator_func(**initPara)

def checkPackageExist(key: str) -> bool:
    """verify a package has been registered"""
    return key in table_creation_funcs

def importModule(module: str) -> Optional[Any]:
    """ loaded the transformation logic into the pipeline"""
    if module is not None:
        return loader.import_module(module)

class deltaTables():
    """PyDelta -  internal delta live table framework"""

    def __init__(self, spark, DLT) -> None:
        """expecting a spark session and DLT(databricks) class

        Args:
            spark (sparkSession): spark session 
            DLT (dlt): delta live table 
        """
        self.DLT = DLT
        self.spark = spark

    def praseArguments(self, arguments: Dict[str, Any]) -> Dict:
        """Paraing the arguments list  """
        res = {}
        # todo: add checking/validation
        # todo: implement by dataclasses
        res['tableName'] = arguments.pop('tableName',None)
        res['fileFormat'] = arguments.pop('fileFormat',None)
        res['sourceTableName'] = arguments.pop('sourceTableName',None)
        res['transformName'] = arguments.pop('transform',None)
        res['modules'] = arguments.pop('modules',None)
        res['instantiation'] = arguments.pop('instantiation',{})
        res['parameter'] = arguments.pop('parameter',{})
        res['dataQuality'] = arguments.pop('dataQuality',{})
        res['returnFormat'] = arguments.pop('returnFormat', None)
        return res

    def getRawTables(self, arguments: Dict[str, Any]) -> dataframe:
        """determate source type of bronze table"""
        
        args = self.praseArguments(arguments)
        return self.getStandRaw(args) if args['fileFormat'] else self.getCustomRaw(args)
        
    def getCustomRaw(self, arguments: Dict[str, Any]) -> dataframe:
        """ load bronze table by python package(using pyspark.sql.SparkSession.createDataFrame)"""
        importModule(arguments['modules'])

        # loader = None
        return_format = arguments.pop('returnFormat', None)
        if return_format == 'pysprak_dataframe':
            transform = createExternalSource(arguments)
            loader_para = arguments['parameter']
        else:
            transform = self.spark.createDataFrame
            loader_para = {'data': arguments['parameter']}
        # loaderPara ={ "data":arguments['parameter']}
        # return_format = arguments.pop('returnFormat', None)
        # transform = self.DeltaPySparkSpark.createDataFrame if return_format != 'pysprak_dataframe' else lambda **x: x.get('data',None)
        # loader = createExternalSource(arguments)
        
        return self.__generateTable(
            # loader,
            arguments['tableName'],
            transform,
            loader_para,
            arguments['dataQuality'],)

    def getStandRaw(self, arguments: Dict[str, Any]) -> dataframe:
        """using spark Generic Load/Save Functions"""
        sourceTablesName ={ "path":arguments['sourceTableName']}
        transform = self.spark.read.format(arguments['fileFormat']).load

        return self.__generateTable(
            # lambda x:x,
            {'name':arguments['tableName']},
        #loader = lambda **x:x['data']
        transform = self.CoxSpark.read.format(arguments['fileFormat']).load
        if type(arguments['tableName']) == dict:
            table_name = arguments['tableName']
        else:
            table_name = {"name":arguments['tableName']}

        return self.__generateTable(
            lambda x:x,
            table_name,
            transform,
            sourceTablesName,
            arguments['dataQuality'],)

    def getIdealTables(self, arguments: Dict[str, Any]) -> dataframe:
        """ transform and load silver tables"""
        args = self.praseArguments(arguments)
        
        importModule(args['modules'])
        sourceTablesName = {args['sourceTableName']:args['sourceTableName']}
        sourceTables = {k: self.DLT.read(*v) for k, v in sourceTablesName.items()}        
        transform = createTransform(args['transformName'])
        
        return self.__generateTable(
            # self.DLT.read
            {'name':args['tableName']},
            transform.transform,
            sourceTables,
            args['dataQuality'],)

    def getBOTables(self, arguments: Dict[str, Any]) -> dataframe:
        """ transform and load gold tables"""

        args = self.praseArguments(arguments)
        
        importModule(args['modules'])
        sourceTablesName = args['sourceTableName']
        sourceTables = {k: self.DLT.read(*v) for k, v in sourceTablesName.items()}
        transform = createTransform(args['transformName'])
        
        return self.__generateTable(
            # self.DLT.read, 
            {'name':args['tableName']},
            transform.transform,
            sourceTables,
            args['dataQuality'],)

    #sourceTables = {k: loader(*v) for k, v in sourceTablesName.items()}
    def __generateTable(
            self,
            # loader,
            table_name,
            transform, 
            sourceTables, 
            DQRules: Dict[str,Any] = {}
        ):
        """generate the delta live table format function"""
        def generate():
            return transform(**sourceTables)
        executor = self.__generateDQFunc(DQRules)(generate)

        return self.DLT.table(**table_name)(executor)

    def __generateDQFunc(self,rules: Dict[str,list[Any]]) -> Callable:
        """apply Data Quality rules
        https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html

        Args:
        rules (Dict[str,Any]): 
            dictionary key is the DQ function(expect, expect_or_drop,etc)
            Value is rules ['description','constraint']

        Returns:
            Callable: a function applied delta live table DQ function
        """
        for func, rule in rules.items():
            DQfunc = getattr(self.DLT, func)
            return DQfunc(*rule)
        return lambda x:x

