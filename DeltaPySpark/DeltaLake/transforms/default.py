
from DeltaPySpark.DeltaLake.transforms.transform import DeltaTable
from DeltaPySpark.DeltaLake.utils import func

class DeltaPipeline(DeltaTable):
    def __init__(self, spark=None, init_conf=None, named_parameter_key_list: list[str] = None, source_table: list[str] = [], table_name: str = "", **kwargs: any):
        super().__init__(spark, init_conf, named_parameter_key_list, source_table, table_name, **kwargs)
        if not table_name: 
            raise NotImplementedError("No target table name")
        if not func.getProperty(self,'logics'): 
            raise NotImplementedError("No transformation has provided")

    def transform(self, **kwarg):
        self.spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", True) # temp location
        return self.logics(**kwarg)
