
from typing import Any, Callable
from abc import ABC, abstractmethod
from DeltaLake.transforms.transform import deltaLiveTable
from pyspark.sql import dataframe

#Callable[..., returntype]
""" factory to prepare the transformation for a table"""

class deltaTables(ABC):

    table_creation_funcs: dict[str, Callable[..., deltaLiveTable]] = {}

    def __init__(self, CoxSpark) -> None:
        self.CoxSpark = CoxSpark

    def register(self, table_name: str, creator_fn: Callable[..., deltaLiveTable]) -> None:
        """Register a table logic type."""
        self.table_creation_funcs[table_name] = creator_fn
    
    @abstractmethod
    def getTables(self, arguments: dict[str, Any]) -> dataframe:
        pass
    
    