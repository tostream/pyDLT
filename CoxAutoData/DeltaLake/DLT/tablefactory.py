
from typing import Any, Callable, Dict
from pyspark.sql import dataframe
from CoxAutoData.DeltaLake.transforms.transform import deltaLiveTable
from CoxAutoData.DeltaLake.DLT import loader

""" using plug-in architecture to seprated tranformation and pipeline"""

table_creation_funcs: Dict[str, Callable[..., deltaLiveTable]] = {}

def register(table_name: str, creator_fn: Callable[..., deltaLiveTable]) -> None:
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

class deltaTables():
    "Cox internal delta live table framework"

    _rawTablesList = {}
    _idealTablesList = {}

    def __init__(self, CoxSpark, CoxDLT) -> None:
        self.CoxDLT = CoxDLT
        self.CoxSpark = CoxSpark

    @property
    def rawTablesList(self):
        return self._rawTablesList
    
    @rawTablesList.setter
    def rawTablesList(self, rawTablesList: Dict):
        self._rawTablesList = rawTablesList

    @property
    def idealTablesList(self):
        return self._idealTablesList
    
    @idealTablesList.setter
    def idealTablesList(self, rawTablesList: Dict):
        self._idealTablesList = rawTablesList

    def importModule(self, module: str):
        """ loaded the transformation logic into the pipeline"""
        if module is not None:
            loader.import_module(module)

    def praseArguments(self, arguments: Dict[str, Any]) -> Dict:
        res = {}
        # todo: add checking/validation
        res['tableName'] = arguments.pop('tableName',None)
        res['fileFormat'] = arguments.pop('fileFormat',None)
        res['sourceTableName'] = arguments.pop('sourceTableName',None)
        res['transformName'] = arguments.pop('transform',None)
        res['modules'] = arguments.pop('modules',None)
        return res

    def getRawTables(self, arguments: Dict[str, Any]) -> dataframe:
        
        para = self.praseArguments(arguments)
        return self.__generateTable(para['tableName'],
            self.CoxSpark.read.format(para['fileFormat']).load(),
            para['sourceTableName'])

    def getIdealTables(self, arguments: Dict[str, Any]) -> dataframe:
        
        para = self.praseArguments(arguments)
        
        self.importModule(para['modules'])
        sourceTablesName = para['sourceTableName']
        transform = createTransform(para['transformName'])
        para = {arguments['sourceTableName'] : self.CoxDLT.read(sourceTablesName)}
        return self.__generateTable(para['tableName'], transform.transform ,para)

    def getBOTables(self, arguments: Dict[str, Any]) -> dataframe:

        para = self.praseArguments(arguments)
        
        self.importModule(para['modules'])
        sourceTablesName = para['sourceTableName']
        transform = createTransform(para['transformName'])
        sourceTables = {k: self.CoxDLT.read(v) for k, v in sourceTablesName.items()}
        return self.__generateTable(para['tableName'], transform.transform ,sourceTables)

    def __generateTable(self, tableName, transform, sourceTables) -> dataframe:
        @self.CoxDLT.table(
            name = tableName
        )
        def generate():
            return transform(**sourceTables)
    