from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, col, rand

def salt_skewed_column(df: DataFrame, skewed_col: str, salt_factor: int = 10) -> DataFrame:
    """
    Add a salting key to a skewed column to distribute data more evenly
    """
    # Add a salting key for skewed values
    return df.withColumn("salt", (rand() * salt_factor).cast("int"))\
             .withColumn(f"{skewed_col}_salted", expr(f"concat({skewed_col}, '_', salt)"))

def unsalt_results(df: DataFrame, original_col: str) -> DataFrame:
    """
    Remove the salting after processing
    """
    return df.drop("salt").withColumn(original_col, expr(f"split({original_col}_salted, '_')[0]"))\
             .drop(f"{original_col}_salted")
