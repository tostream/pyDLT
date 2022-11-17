
from typing import Any
from pyspark.sql import dataframe
from DeltaLake.DLT.tablefactory import deltaTables

class Bronze(deltaTables):

    def getTables(self, arguments: dict[str, Any]) -> dataframe:
        tableName = arguments.pop('tableName')
        fileFormat = arguments.pop('fileFormat')
        sourceTableName = arguments.pop('sourceTableName')
        @self.CoxSpark.table(
            name = f"{tableName}"
        )
        def generateBronze():
            return self.CoxSpark.read.format(fileFormat).load(sourceTableName)