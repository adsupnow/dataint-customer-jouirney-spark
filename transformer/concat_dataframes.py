import pandas as pd
from typing import List
from flytekit import task, workflow, Resources, ImageSpec
from pyspark.sql import DataFrame

def transform(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    Concatenates a list of pyspark DataFrames into a single DataFrame.

    This task appends DataFrames row-wise. It resets the index of the
    resulting DataFrame to be continuous from 0 to N-1.

    Args:
        dfs_to_concat: A list of one or more pandas DataFrames.
        join_strategy: The join strategy to use ('outer' or 'inner').
                       'outer' keeps all columns, filling non-matches with NaN.
                       'inner' keeps only columns present in all DataFrames.

    Returns:
        A single pandas DataFrame containing the combined data.
    """
    
    concatenated_df = df1.unionByName(df2, allowMissingColumns=True)
    
    # Drop duplicates in name and master_id
    concatenated_df = concatenated_df.drop_duplicates(subset=['name', 'master_id'])

    return concatenated_df 