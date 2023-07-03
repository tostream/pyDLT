from typing import Optional, Type
from pyspark.sql import SparkSession

def get_dbutils(spark: SparkSession) -> Optional[Type]:
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except ImportError:
        return None
