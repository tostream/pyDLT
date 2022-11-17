
from abc import ABC, abstractmethod

from pyspark.sql import dataframe


class deltaLiveTable(ABC):

    tableName = ''

    def __init__(self, tableName):
        self.tableName = tableName

    @abstractmethod
    def transform(self, **SourceTable: dataframe) -> dataframe:
        pass
