
from typing import Dict, Any, TypeVar, Optional
from abc import ABC, abstractmethod
from argparse import ArgumentParser
from re import search
import sys,yaml, pathlib
from pyspark.sql import SparkSession, DataFrame, DataFrameWriter, dataframe
from DeltaPySpark.DeltaLake.utils import func, writer
from DeltaPySpark.DeltaLake.Delta.deltaTable import default_conf

T = TypeVar('T', bound=DataFrame|DataFrameWriter)

def get_dbutils(
    spark: SparkSession,
):
    """Get the DBUtils object to ease mocking. used in mocking by its name.
    Please note that this function is  used in mocking by its name.

    Args:
        spark (SparkSession): The SparkSession object.

    Returns:
        DBUtils or None: An instance of the DBUtils class if it is available,
        otherwise None.
    """

    try:
        from pyspark.dbutils import DBUtils  # noqa

        if "dbutils" not in locals():
            utils = DBUtils(spark)
            return utils
        else:
            return locals().get("dbutils")
    except ImportError:
        return None




class DeltaTable(ABC):

    """
    This is an abstract class that provides handy interfaces to implement workloads (e.g. jobs or job tasks).
    Create a child from this class and implement the abstract launch method.
    Class provides access to the following useful objects:
    * self.spark is a SparkSession
    * self.dbutils provides access to the DBUtils
    * self.logger provides access to the Spark-compatible logger
    * self.conf provides access to the parsed configuration of the job
    * self.widgets provides access to the retrieved widgets
    """

    def __init__(
        self,
        spark=None,
        init_conf=None,
        named_parameter_key_list: list[str] = None,
        source_table: list[str] = [],
        table_name: str = "",
        **kwargs: any
        
    ):
        """
        Initializes a Task object.

        Args:
            spark: The SparkSession object to use for processing.
            init_conf: The initial configuration for the Task.
            named_parameter_key_list: A list of named parameter keys to extract configurational arguments.
            source_table: list of source for transformation to use
            table_name: overrider table name (default use class name)
        Returns:
            None
        """

        self.spark = self._prepare_spark(spark)
        self.logger = self._prepare_logger()
        self.dbutils = self.get_dbutils()
        if init_conf:
            self.conf = init_conf
        else:
            self.conf = self._provide_config(named_parameter_key_list)

        self.source_table = source_table
        if table_name:
            self.table_name = table_name
        else:
            self.table_name = self.__class__.__name__
        self._log_conf()
        self.__dict__.update(kwargs)


    @staticmethod
    def _prepare_spark(spark) -> SparkSession:
        if not spark:
            return SparkSession.builder.getOrCreate()
        else:
            return spark
        
    @staticmethod
    def check_schema(table_name:str ) -> bool:
        return len(table_name.split(".")) > 1

    def get_dbutils(self):
        """Returns the DBUtils instance.

        This method retrieves the DBUtils instance associated with the Spark session.

        Returns:
            DBUtils: The DBUtils instance.
        """
        utils = get_dbutils(self.spark)

        if not utils:
            self.logger.warn("No DBUtils defined in the runtime")
        else:
            self.logger.info("DBUtils class initialized")

        return utils

    def _provide_config(self, named_parameter_key_list: [str] = None):
        self.logger.info(
            "Reading configuration from --conf-file job option or named parameters"
        )
        conf_file = self._get_conf_file()
        if not conf_file:
            if named_parameter_key_list:
                self.logger.info(
                    f"Collecting named parameters: {named_parameter_key_list}"
                )
                parser = ArgumentParser()

                for parameter_name in named_parameter_key_list:
                    parser.add_argument(
                        f"--{parameter_name}", dest=parameter_name, default=None, required=False
                    )
                
                parsed_args = None
                
                # pytest does not like empty sys.argv
                if search('pytest', sys.argv[0]):
                    parsed_args = parser.parse_args([])
                else:
                    parsed_args = parser.parse_args()

                extracted_parameters = dict()

                for parameter_name in named_parameter_key_list:
                    extracted_parameters[parameter_name] = getattr(parsed_args, parameter_name)

                return extracted_parameters
            else:
                self.logger.info(
                    "No conf file was provided, setting configuration to empty dict."
                    "Please override configuration in subclass init method"
                )
                return {}
        else:
            self.logger.info(
                f"Conf file was provided, reading configuration from {conf_file}"
            )
            return self._read_config(conf_file)

    @staticmethod
    def _get_conf_file():
        p = ArgumentParser()
        p.add_argument("--conf-file", required=False, type=str)
        namespace = p.parse_known_args(sys.argv[1:])[0]
        return namespace.conf_file

    @staticmethod
    def _read_config(conf_file) -> Dict[str, Any]: 
        config = yaml.safe_load(pathlib.Path(conf_file).read_text())
        return config

    def _prepare_logger(self):
        log4j_logger = self.spark._jvm.org.apache.log4j  # noqa
        return log4j_logger.LogManager.getLogger(self.__class__.__name__)

    def _log_conf(self):
        # log parameters
        self.logger.info("Launching job with configuration parameters:")
        for key, item in self.conf.items():
            self.logger.info("\t Parameter: %-30s with value => %-30s" % (key, item))

    @abstractmethod
    def transform(self, **df: dict[str, dataframe]):
        """
        Main method of the job.
        :return:
        """
        pass

    def launch(self):
        table_name = self.table_name if self.table_name else self.__class__.__name__
        self.logger.info(f"Launching {table_name} ETL task")
        source_df = self.get_source_tables(self.source_table)
        df = self.transform(**source_df)
        if df:
            if func.getProperty(self,'primary_keys'):
                partitioning_cols = func.getProperty(self,'partitioning_cols')
                self._merge_detla_table(table_name , df, self.primary_keys, partitioning_cols)
            else:
                self._save_table(table_name , df)
        self.logger.info("{table_name} ETL task finished!")

    def _get_raw(self,table_name) -> dataframe:
        self.logger.info(f"'{table_name}' table is populated")
        table_name = self.get_table_name(table_name)
        return self.spark.table( table_name)
    
    def get_source_tables(self, table_names: list) -> dict:
        res = {}
        for table in table_names:
            res[table] = self._get_raw(table)
        return res

    def get_catalag_name(self) -> str:
        return func.getProperty("catalog",func.getSparkConf("catalog","dev"))
    
    def get_schema_name(self) -> str:
        return func.getProperty("schema",func.getSparkConf("schema","default"))

    def get_table_name(self, table_name) -> str:
        if not self.check_schema(table_name):
            table_name = f"{self.get_schema_name()}.{table_name}"

        table_name = f"{self.get_catalag_name()}.{table_name}"
        return table_name

    def _save_table(self,table_name:str, df: dataframe, *option_arg:Any, **option_karg: Any) -> None:
        table_name = self.get_table_name(table_name)
        df_writer = func.parse_default_arg(df.write,default_conf,self.spark)
        df_writer.saveAsTable(table_name,*option_arg, **option_karg)
        self.logger.info(f"'{table_name}'table is populated")

    def _merge_detla_table(self,
                           table_name,
                           df,
                           primary_keys,
                           partitioning_cols: Optional[list]=None,):
        query = func.getTableExistsQuery(table_name,self.get_schema_name(),self.get_catalag_name())
        table_name = self.get_table_name(table_name)
        if (func.tableExists(query)):
            merge_query = writer.getMergeQuery(primary_keys,partitioning_cols)
            writer.deltaMerge(df,table_name,merge_query)
        else:
            writer.saveNonExistsTable(table_name,df,partitioning_cols)
