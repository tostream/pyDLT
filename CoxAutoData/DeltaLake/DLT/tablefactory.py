
from typing import Any, Callable, Dict
from abc import ABC, abstractmethod
from ..transforms.transform import deltaLiveTable
from pyspark.sql import dataframe

""" factory to prepare the transformation for a table"""

class deltaTables(ABC):

    table_creation_funcs: Dict[str, Callable[..., deltaLiveTable]] = {}

    def __init__(self, CoxSpark, CoxDLT) -> None:
        self.CoxDLT = CoxDLT
        self.CoxSpark = CoxSpark

    def register(self, table_name: str, creator_fn: Callable[..., deltaLiveTable]) -> None:
        """Register a table logic type."""
        self.table_creation_funcs[table_name] = creator_fn
    
    @abstractmethod
    def getTables(self, arguments: Dict[str, Any]) -> dataframe:
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
        tableName = arguments.pop('tableName')
        transform = arguments.pop('transform')
        sourceTableName = arguments.pop('sourceTableName')
        @self.CoxDLT.table(
            name = f"{tableName}"
        )
        def generatesilver():
            return transform(self.CoxDLT.read(sourceTableName))


class gold(deltaTables):

    def getTables(self, arguments: Dict[str, Any]) -> dataframe:
        tableName = arguments.pop('tableName')
        transform = arguments.pop('transform')
        sourceTablesName = arguments.pop('sourceTableName')
        for sourceTable in sourceTablesName:
            sourceTablesName.update({sourceTable : self.CoxDLT.read(sourceTablesName[sourceTable])})
        @self.CoxDLT.table(
            name = f"{tableName}"
        )
        def generateGold():
            return transform(**sourceTablesName)