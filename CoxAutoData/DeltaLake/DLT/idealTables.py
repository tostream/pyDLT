
from typing import Any
from pyspark.sql import dataframe
from DeltaLake.DLT.tablefactory import deltaTables


class silver(deltaTables):

    def getTables(self, arguments: dict[str, Any]) -> dataframe:
        tableName = arguments.pop('tableName')
        transform = arguments.pop('transform')
        sourceTableName = arguments.pop('sourceTableName')
        @self.CoxSpark.table(
            name = f"{tableName}"
        )
        def generatesilver():
            return transform(self.CoxSpark.read(sourceTableName))
