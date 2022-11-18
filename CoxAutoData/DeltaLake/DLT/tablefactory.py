
from typing import Any, Callable, Dict
from abc import ABC, abstractmethod
from pyspark.sql import dataframe
from CoxAutoData.DeltaLake.transforms.transform import deltaLiveTable
from CoxAutoData.DeltaLake.DLT import loader

""" factory to prepare the transformation for a table"""


table_creation_funcs: Dict[str, Callable[..., deltaLiveTable]] = {}

def register(table_name: str, creator_fn: Callable[..., deltaLiveTable]) -> None:
    """Register a table logic type."""
    table_creation_funcs[table_name] = creator_fn

def createTransform(arguments: Dict[str, Any]) -> deltaLiveTable:
    #args_copy = arguments.copy()
    transform = arguments.pop("transform")
    try:
        creator_func = table_creation_funcs[transform]
    except KeyError:
        raise ValueError(f"unknown character type {transform!r}") from None
    return creator_func(transform)

class deltaTables(ABC):


    def __init__(self, CoxSpark, CoxDLT) -> None:
        self.CoxDLT = CoxDLT
        self.CoxSpark = CoxSpark


    @abstractmethod
    def getTables(self, arguments) -> dataframe:
        pass


class Bronze(deltaTables):

    def getTables(self, arguments: Dict[str, Any]) -> dataframe:
        tableName = arguments.pop('tableName')
        fileFormat = arguments.pop('fileFormat')
        sourceTableName = arguments.pop('sourceTableName')
        @self.CoxDLT.table(
            name = f"{tableName}"
        )
        def generateBronze():
            return self.CoxSpark.read.format(fileFormat).load(sourceTableName)


class silver(deltaTables):

    def getTables(self, arguments: Dict[str, Any]) -> dataframe:
        
        # import the silver package
        modules = arguments.pop('modules')
        loader.import_module(modules)
        #get ideal name
        tableName = arguments.pop('tableName')
        sourceTableName = arguments.pop('sourceTableName')
        #get transformation mapping(dict)
        transformName = arguments.pop('transform')
        transform = createTransform(transformName)
        @self.CoxDLT.table(
            name = f"{tableName}"
        )
        def generatesilver():
            table = self.CoxDLT.read(sourceTableName)
            return transform.transform(table)


class gold(deltaTables):

    def getTables(self, arguments: Dict[str, Any]) -> dataframe:
        modules = arguments.pop('modules')
        loader.import_module(modules)
        tableName = arguments.pop('tableName')
        transform = createTransform(arguments.pop('transform'))
        sourceTablesName = arguments.pop('sourceTableName')
        @self.CoxDLT.table(
            name = f"{tableName}"
        )
        def generateGold():
            para ={}
            for sourceTable in sourceTablesName:
                value=self.CoxDLT.read(sourceTablesName[sourceTable])
                para[sourceTable]=value
            #    sourceTablesName.update({sourceTable : self.CoxDLT.read(sourceTablesName[sourceTable])})
            return transform.transform(**para)