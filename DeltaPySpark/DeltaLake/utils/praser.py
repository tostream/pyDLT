""" utils tool"""
from typing import Optional, Callable
from datetime import datetime
import functools as ft
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

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
    def prase_column(df: DataFrame, column: dict):
        return df.withColumn(column['key'], transformation(column['value']))
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
                get_flattened_cols(df.selectExpr(f"explode({t}) as {t}"))
                array_col_list.append(t)
            else:
                flattened_col_list.append(f"{t} as {t.replace('.','_')}")

    def explode_array(df: DataFrame, cols_list):
        for col in cols_list:
            df = df.withColumn(col, expr(f"explode({col})"))
        return df

    # Call the inner Method
    get_flattened_cols(_df)
    res_df = explode_array(_df, array_col_list)
    # Return the flattened Data Frame
    return res_df.selectExpr(flattened_col_list)
