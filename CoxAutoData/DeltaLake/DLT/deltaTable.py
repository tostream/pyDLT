from typing import Optional
from pyspark.sql import SparkSession


class deltaTable:
    """ Cox delta lake interputer"""
    def __init__(self, cox_spark: SparkSession) -> None:
        self.cox_spark = cox_spark

    def table(self,name: Optional[str] = None):
        """ create a delta table"""
        pass

