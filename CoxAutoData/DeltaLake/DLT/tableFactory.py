
from typing import Any, Callable, Dict

from pyspark.sql import dataframe

from CoxAutoData.DeltaLake.DLT import loader
from CoxAutoData.DeltaLake.transforms.transform import deltaLiveTable

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

def importModule(module: str):
    """ loaded the transformation logic into the pipeline"""
    if module is not None:
        loader.import_module(module)

class deltaTables():
    """CoxPyDelta - Cox internal delta live table framework"""

    def __init__(self, CoxSpark, CoxDLT) -> None:
        """expecting a spark session and DLT(databricks) class

        Args:
            CoxSpark (sparkSession): spark session 
            CoxDLT (dlt): delta live table 
        """
        self.CoxDLT = CoxDLT
        self.CoxSpark = CoxSpark

    def praseArguments(self, arguments: Dict[str, Any]) -> Dict:
        """Paraing the arguments list  """
        res = {}
        # todo: add checking/validation
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

        loaderPara ={ "data":arguments['parameter']}
        return_format = arguments.pop('returnFormat', None)
        transform = self.CoxSpark.createDataFrame if return_format != 'pysprak_dataframe' else lambda **x: x.get('data',None)
        loader = createExternalSource(arguments)
        
        return self.__generateTable(
            loader,
            arguments['tableName'],
            transform,
            loaderPara,
            arguments['dataQuality'],)

    def getStandRaw(self, arguments: Dict[str, Any]) -> dataframe:
        """using spark Generic Load/Save Functions"""
        sourceTablesName ={ "path":arguments['sourceTableName']}
        transform = self.CoxSpark.read.format(arguments['fileFormat']).load

        return self.__generateTable(
            lambda x:x,
            arguments['tableName'],
            transform,
            sourceTablesName,
            arguments['dataQuality'],)

    def getIdealTables(self, arguments: Dict[str, Any]) -> dataframe:
        """ transform and load silver tables"""
        args = self.praseArguments(arguments)
        
        importModule(args['modules'])
        sourceTablesName = {args['sourceTableName']:args['sourceTableName']}
        transform = createTransform(args['transformName'])
        
        return self.__generateTable(
            self.CoxDLT.read,
            args['tableName'],
            transform,
            sourceTablesName,
            args['dataQuality'],)

    def getBOTables(self, arguments: Dict[str, Any]) -> dataframe:
        """ transform and load gold tables"""

        args = self.praseArguments(arguments)
        
        importModule(args['modules'])
        sourceTablesName = args['sourceTableName']
        transform = createTransform(args['transformName'])
        
        return self.__generateTable(
            self.CoxDLT.read, 
            args['tableName'],
            transform.transform,
            sourceTablesName,
            args['dataQuality'],)

    def __generateTable(
            self,
            loader,
            tableName,
            transform, 
            sourceTablesName, 
            DQRules: Dict[str,Any] = {}
        ):
        """generate the delta live table format function"""
        def generate():
            sourceTables = {k: loader(*v) for k, v in sourceTablesName.items()}
            return transform(**sourceTables)
        executor = self.__generateDQFunc(DQRules)(generate)

        return self.CoxDLT.table(name=tableName)(executor)

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
            DQfunc = getattr(self.CoxDLT, func)
            return DQfunc(*rule)
        return lambda x:x

