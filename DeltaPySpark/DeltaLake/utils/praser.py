""" utils tool"""
from typing import Optional, Callable
from datetime import datetime
import functools as ft
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, column, explode

def prase_EpochDate_str(name: str) -> Optional[str]:
    return prase_epochdate_str(name)

def prase_epochdate_str(name: str) -> Optional[str]:
    """ parse Unix Datetime to stand format"""
    return datetime.fromtimestamp(int(name)).strftime('%Y-%m-%d') if name.isdecimal() else None


def prase_columns_dateformat(
        df: DataFrame,
        columns: list[dict],
        transformation: Callable) -> DataFrame:
    """ iterate column list and praseing the format"""
    def prase_column(df: DataFrame, col: dict):
        return df.withColumn(col['key'], transformation(col['value']))
    return ft.reduce(prase_column, columns, df)


def flatten_json_df(_df: DataFrame):
    # List to hold the dynamically generated column names
    flattened_col_list = []
    array_col_list = []
    # Inner method to iterate over Data Frame to generate the column list

    def get_flattened_cols(df: DataFrame, struct_col: Optional[str] = None) -> None:
        for col in df.columns:
            t = col if struct_col is None else struct_col + "." + col
            if df.schema[col].dataType.typeName() == 'struct':
                get_flattened_cols(df.select(col+".*"), t)
            elif df.schema[col].dataType.typeName() == 'array':
                array_col_list.append(t)
                get_flattened_cols( df.select(explode(column(col)).alias(col)).select(col+".*"),t.replace('.','_'))
                # get_flattened_cols(df.select(explode(column(col)).alias(col)),t)
            else:
                flattened_col_list.append(column(t).alias(t.replace('.','_')))

    def explode_array(df: DataFrame, cols_list):
        for col in cols_list:
            # df = df.withColumn(col, expr(f"explode({col})"))
            df = df.withColumn(col.replace('.','_'), expr(f"explode({col})"))
        return df

    # Call the inner Method
    get_flattened_cols(_df)
    res_df = explode_array(_df, array_col_list)
    # Return the flattened Data Frame
    return res_df.select(flattened_col_list)
