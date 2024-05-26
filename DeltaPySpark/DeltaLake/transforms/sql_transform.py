from DeltaPySpark.DeltaLake.transforms.transform import DeltaTable
from DeltaPySpark.DeltaLake.utils import func

class SQLPipeline(DeltaTable):
    """pyspark pipeline for SQL
    require para:
        table_name: target table name
        sql: query for execite
    optional para:
        catalog: default catalog
        schema: default schema

    """
    def __init__(self, spark=None, init_conf=None, named_parameter_key_list: list[str] = None, source_table: list[str] = ..., table_name: str = "", **kwargs: any):
        super().__init__(spark, init_conf, named_parameter_key_list, source_table, table_name, **kwargs)
        if not table_name: 
            raise NotImplementedError("No target table name")
        if not func.getProperty(self,'sql'): 
            raise NotImplementedError("No query provided")
        
        if func.getProperty(self,'catalog'): 
            self.spark.sql(f"use catalog {self.catalog}")
        else:
            self.spark.sql(f"use catalog {self.get_catalag_name()}")

        if func.getProperty(self,'schema'): 
            self.spark.sql(f"use schema {self.schema}")
        elif func.getProperty(self,'databases'): 
            self.spark.sql(f"use schema {self.databases}")
        else:
            self.spark.sql(f"use schema {self.get_schema_name()}")

    def transform(self):
        self.spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", True) # temp location
        return self.spark.sql(self.sql)
