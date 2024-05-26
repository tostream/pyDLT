from typing import Optional
from pyspark.sql import SparkSession

default_conf = [
    [["delta.mergeSchema",""], lambda val: {"option":["mergeSchema", val]}, True ],
    [["delta.writeMode",""], lambda val: {"mode": val}, "overwrite"],
]

class deltaTable:
    """ delta lake interputer"""
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def table(self,name: Optional[str] = None):
        """ create a delta table"""
        pass

