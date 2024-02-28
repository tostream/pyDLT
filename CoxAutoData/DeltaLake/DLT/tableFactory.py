
from typing import Any, Callable, Dict, Optional

from pyspark.sql import DataFrame

from CoxAutoData.DeltaLake.DLT import loader
from CoxAutoData.DeltaLake.transforms.transform import deltaLiveTable

from enum import Enum

class FlowLayer(Enum):
    RAW = "raw"
    IDEAL = "ideal"
    BO = "businessObject"
""" using plug-in architecture to separated tranformation and pipeline"""

table_creation_funcs: Dict[str, Callable[..., Any]] = {}

def register(table_name: str, creator_fn: Callable[..., Any]) -> None:
    """Register a transformation.

    Args:
        table_name (str): name of the register table
        creator_fn (Callable[..., Any]): Object of pyspark table transformation logic
    """
    table_creation_funcs[table_name] = creator_fn

def createTransform(arguments: Dict[str, Any]) -> deltaLiveTable:
    """_summary_ get the transformation logic

    Args:
        arguments (Dict[str, Any]): _description_

    Raises:
        ValueError: _description_

    Returns:
        deltaLiveTable: _description_
    """

    transform = arguments.pop("transform")
    try:
        creator_func = table_creation_funcs[transform]
    except KeyError:
        raise ValueError(f"unknown character type {transform!r}") from None
    return creator_func(transform)

def createExternalSource(arguments: Dict[str, Any]) -> Callable:
    """_summary_
    get the python package retrieve data by customer python package

    Args:
        arguments (Dict[str, Any]): _description_

    Raises:
        ValueError: _description_

    Returns:
        Callable: _description_
    """

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
    
    def runFlow(self, arguments: Dict[str, Any],flowLayer: str ) -> DataFrame:
        if flowLayer.lower() == "ideal":
            self.getIdealTables(arguments)
        elif flowLayer.lower() == "bo":
            self.getBOTables(arguments)
        else :
            self.getRawTables(arguments)


    def getRawTables(self, arguments: Dict[str, Any]) -> DataFrame:
        """determate source type of bronze table"""
        
        args = self.praseArguments(arguments)
        return self.getStandRaw(args) if args['fileFormat'] else self.getCustomRaw(args)
        
    def getCustomRaw(self, arguments: Dict[str, Any]) -> DataFrame:
        """ load bronze table by python package(using pyspark.sql.SparkSession.createDataFrame)"""
        importModule(arguments['modules'])

        loader = lambda x:x,
        return_format = arguments.pop('returnFormat', None)
        if return_format == 'pysprak_dataframe':
            transform = createExternalSource(arguments)
            loader_para = arguments['parameter']
        else:
            transform = self.CoxSpark.createDataFrame
            loader_para = {'data': arguments['parameter']}

        return self.__generateTable(
            loader,
            arguments['tableName'],
            transform,
            loader_para,
            arguments['dataQuality'],)

    def getStandRaw(self, arguments: Dict[str, Any]) -> DataFrame:
        """_summary_ 
            Using spark Generic Load/Save Functions
            
        Args:
            arguments (Dict[str, Any]): _description_

        Returns:
            DataFrame: _description_
        """
        sourceTablesName ={ "path":arguments['sourceTableName']}
        #loader = lambda **x:x['data']
        transform = self.CoxSpark.read.format(arguments['fileFormat']).load

        return self.__generateTable(
            lambda x:x,
            arguments['tableName'],
            transform,
            sourceTablesName,
            arguments['dataQuality'],)

    def getIdealTables(self, arguments: Dict[str, Any]) -> DataFrame:
        """ transform and load silver tables"""
        args = self.praseArguments(arguments)
        
        importModule(args['modules'])
        sourceTablesName = {args['sourceTableName']:args['sourceTableName']}
        #sourceTables = {k: self.CoxDLT.read(v) for k, v in sourceTablesName.items()}        
        transform = createTransform(args['transformName'])
        
        return self.__generateTable(
            self.CoxDLT.read,
            args['tableName'],
            transform.transform,
            sourceTablesName,
            args['dataQuality'],)

    def getBOTables(self, arguments: Dict[str, Any]) -> DataFrame:
        """ transform and load gold tables"""

        args = self.praseArguments(arguments)
        
        importModule(args['modules'])
        sourceTablesName = args['sourceTableName']
        #sourceTables = {k: self.CoxDLT.read(v) for k, v in sourceTablesName.items()}
        transform = createTransform(args[ 'transformName'])
        
        return self.__generateTable(
            self.CoxDLT.read, 
            args['tableName'],
            transform.transform,
            sourceTablesName,
            args['dataQuality'],)

    #sourceTables = {k: loader(*v) for k, v in sourceTablesName.items()}
    def __generateTable(
            self,
            loader,
            table_name,
            transform,
            source_tables_name,
            dq_rules: Dict[str,Any] = {}
        ): 
        """generate the delta live table format function"""
        def generate():
            source_tables = {k: loader(v) for k, v in source_tables_name.items()}
            return transform(**source_tables)
        executor = self.__generateDQFunc(dq_rules)(generate)

        return self.CoxDLT.table(**table_name)(executor)

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
            dq_func = getattr(self.CoxDLT, func)
            return dq_func(*rule)
        return lambda x:x

