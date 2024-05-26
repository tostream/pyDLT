from typing import Optional
from dataclasses import dataclass
from pyspark.sql import SparkSession, dataframe, Window
from pyspark.sql.functions import monotonically_increasing_id, row_number
from delta.tables import DeltaTable, DeltaMergeBuilder
from DeltaPySpark.DeltaLake.utils.func import addAction, getTableExistsQuery, tableExists

def getMergeQuery(primary_keys:list, partitioning_cols: Optional[list] = None) -> str:
    """generate condition SQL for merge operation

    Args:
        primary_keys (list): _description_
        partitioning_cols (Optional[list], optional): _description_. Defaults to None.

    Returns:
        str: practical SQL
    """
    merge_query = " AND ".join([f"src.{key} = dst.{key}" for key in primary_keys])
    
    # extend merge query with partition pruning if configured
    if partitioning_cols:
        partition_prune_query = " AND ".join([f"src.{partition} = dst.{partition}" for partition in partitioning_cols])
        merge_query = merge_query + " AND " + partition_prune_query
    
    return merge_query

def getUpdateCondition(modified_col: str|list[str] = None) -> str:
    """generate condition SQL for delta merge

    Args:
        modified_col (str | list[str], optional): _description_. Defaults to None.

    Returns:
        str: _description_
    """
    match modified_col:
        case isinstance(modified_col,str):
            return f"src.{modified_col} > dst.{modified_col}" 
        case isinstance(modified_col,list):
            return " AND ".join([f"src.{col} = dst.{col}" for col in modified_col])
        case _: return None

def saveNonExistsTable(
        TableName: str,
        BatchDf: dataframe,
        partitioning_cols: Optional[list]=None,
        dropDuplicates: Optional[bool]=False,):
    """_summary_

    Args:
        TableName (str): _description_
        BatchDf (dataframe): _description_
        partitioning_cols (Optional[list], optional): _description_. Defaults to None.
        dropDuplicates (Optional[bool], optional): _description_. Defaults to False.
    """
    writer = BatchDf
    if dropDuplicates:
        writer = addAction(writer,"dropDuplicates",)
    writer = writer.write
    if partitioning_cols:
        writer = addAction(writer,"partitionBy",*partitioning_cols)
    writer.saveAsTable(TableName) 

def _deltaMergeBuilder(microBatchOutputDF: dataframe, 
                       dst_loc:str , 
                       merge_query: str, 
                       sparkSess:SparkSession = SparkSession.getActiveSession()) -> DeltaMergeBuilder:
    """_summary_

    Args:
        microBatchOutputDF (dataframe): _description_
        dst_loc (str): _description_
        merge_query (str): _description_
        sparkSess (SparkSession, optional): _description_. Defaults to SparkSession.getActiveSession().

    Returns:
        _type_: _description_
    """
    # if not sparkSess:
    #     sparkSess = SparkSession.getActiveSession()
    if not sparkSess: raise Exception("no active spark session")
    DstTable = DeltaTable.forName(sparkSess, dst_loc)
    return DstTable.alias("dst").merge(
                source = microBatchOutputDF.alias("src"),
                condition = merge_query
            )

def deltaMerge(microBatchOutputDF: dataframe,
               dst_loc: str, 
               merge_query: str, 
               update_condition: Optional[str] = None,
               sparkSess:SparkSession = SparkSession.getActiveSession()) -> None:
    """_summary_

    Args:
        microBatchOutputDF (dataframe): _description_
        dst_loc (str): _description_
        merge_query (str): _description_
        update_condition (Optional[str], optional): _description_. Defaults to None.
        sparkSess (SparkSession, optional): _description_. Defaults to SparkSession.getActiveSession().
    """
    (_deltaMergeBuilder(microBatchOutputDF, dst_loc, merge_query,sparkSess)
            .whenNotMatchedInsertAll()
            .whenMatchedUpdateAll(condition=update_condition)
            .execute()
    )

def deltaInsert(microBatchOutputDF:dataframe,
                dst_loc:str, 
                merge_query:str, 
                sparkSess:SparkSession = SparkSession.getActiveSession()) -> None:
    """_summary_

    Args:
        microBatchOutputDF (dataframe): _description_
        dst_loc (str): _description_
        merge_query (str): _description_
        sparkSess (SparkSession, optional): _description_. Defaults to SparkSession.getActiveSession().
    """
    (_deltaMergeBuilder(microBatchOutputDF, dst_loc, merge_query,sparkSess)
        .whenNotMatchedInsertAll()
        .execute()
    )


@dataclass
class MergeToDelta():
    """ helper class to dealing with delta table
    dst_catalog: str - destination catalog name
    dst_schema: str - destination schema name
    dst_table: str - destination table name

    Returns:
        obj: MergeToDelta instance
    """
    dst_catalog: str 
    dst_schema: str
    dst_table: str
    primary_keys: Optional[list[str]] = None
    modified_col: Optional[str] = None
    partitioning_cols: Optional[list[str]] = None
    create_cols: Optional[dict[str]] = None

    def __post_init__(self):
        """construct dst_loc - destination location,
        dst_changelog_loc - destination change log destination (type 2 table)
        """
        self.dst_loc = f"{self.dst_catalog}.{self.dst_schema}.{self.dst_table}"
        self.dst_changelog_loc = f"{self.dst_catalog}.{self.dst_schema}.{self.dst_table}_changelog"


    def tableExists(self, suffix:str = "") -> bool:
        """check table exists

        Args:
            suffix (str, optional): _description_. Defaults to "".

        Returns:
            _type_: _description_
        """
        query = getTableExistsQuery(f'{self.dst_table}{suffix}',self.dst_schema,self.dst_catalog)
        return tableExists(query)

    def rollupbatches(self, batchDf: dataframe) -> dataframe:
        """roll up(upsert) a micros batches (datagrame) to prevent duplicate records

        Args:
            batchDf (dataframe): _description_

        Returns:
            dataframe: _description_
        """
        modified_col = self.modified_col
        if not modified_col:
            modified_col = monotonically_increasing_id()
        w = Window.partitionBy(self.primary_keys).orderBy(self.modified_col)
        return (batchDf.dropDuplicates().withColumn("rownum", row_number().over(w))
            .where("rownum = 1").drop("rownum"))

    def mergeSetup(self):
        """prepare merge query and update condition
        """
        self.merge_query = getMergeQuery(self.primary_keys,self.partitioning_cols)
        self.update_condition = getUpdateCondition(self.modified_col)

    def mergeToType1(self, microBatchOutputDF: dataframe, batchId: int) -> None:
        """_summary_

        Args:
            microBatchOutputDF (dataframe): _description_
            batchId (int): _description_
        """
        batchDf = self.rollupbatches(microBatchOutputDF.dropDuplicates())
        if self.tableExists():
            self.mergeSetup()
            deltaMerge(batchDf,self.dst_loc,self.merge_query,self.update_condition)
        else:
            saveNonExistsTable(self.dst_loc,batchDf,self.partitioning_cols)

    def appendToChangelog(self, microBatchOutputDF: dataframe, batchId: int) -> None:
        """_summary_

        Args:
            microBatchOutputDF (dataframe): _description_
            batchId (int): _description_
        """
        batchDf = microBatchOutputDF.dropDuplicates()
        if self.tableExists("_changelog"):
            (
                batchDf.write
                .mode("append")
                #.partitionBy(*self.partitioning_cols)
                .saveAsTable(self.dst_changelog_loc)
            )
        else:
            saveNonExistsTable(self.dst_changelog_loc,batchDf,self.partitioning_cols)


    def appendMerge(self, microBatchOutputDF: dataframe, batchId: int) -> None:
        """_summary_

        Args:
            microBatchOutputDF (dataframe): _description_
            batchId (int): _description_
        """
        batchDf = self.rollupbatches(microBatchOutputDF.dropDuplicates())
        if self.tableExists():
            self.mergeSetup()
            deltaInsert(batchDf,self.dst_loc,self.merge_query)
        else:
            saveNonExistsTable(self.dst_loc,batchDf,self.partitioning_cols)