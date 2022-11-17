
from typing import Any
from pyspark.sql import dataframe
from DeltaLake.DLT.tablefactory import deltaTables


class gold(deltaTables):

    def getTables(self, arguments: dict[str, Any]) -> dataframe:
        tableName = arguments.pop('tableName')
        transform = arguments.pop('transform')
        sourceTablesName = arguments.pop('sourceTableName')
        for sourceTable in sourceTablesName:
            sourceTablesName.update({sourceTable : self.CoxSpark.read(sourceTablesName[sourceTable])})
        @self.CoxSpark.table(
            name = f"{tableName}"
        )
        def generateGold():
            return transform(**sourceTablesName)