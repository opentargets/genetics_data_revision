from pathlib import Path
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def get_cwd():
    """
    It returns the current working directory
    """
    return Path.cwd()

def get_datasets_paths(bucket: str, datasets: list):
    """
    It takes a bucket name and a list of datasets and returns a list of tuples where each tuple contains
    the name of the dataset and the path to the dataset in the bucket
    
    Args:
      bucket (str): the name of the bucket where the datasets are stored
      datasets (list): list of datasets to be downloaded
    
    Returns:
      A list of tuples, where each tuple is a dataset name and the path to that dataset in the bucket.
    """
    return [(data, f"{bucket}/{data}") for data in datasets]

def camel_case_col_names(df: DataFrame) -> DataFrame:
    """
    "Convert the column names of a DataFrame to camel case."
    
    The function is a composition of two functions: `with_columns_renamed` and `convert_snake_to_camel`
    
    Args:
      df (DataFrame): DataFrame
    
    Returns:
      A function that takes a DataFrame and returns a DataFrame
    """
    return with_columns_renamed(convert_snake_to_camel)(df)

def with_columns_renamed(fun):
    """
    It takes a function that takes a column name and returns a new column name, and returns a function
    that takes a dataframe and returns a new dataframe with the columns renamed

    Method extracted from quinn
    
    Args:
      fun: a function that takes a column name and returns a new column name
    
    Returns:
      A function that takes a dataframe and returns a dataframe with the columns renamed.
    """
    def _(df):
        cols = list(
            map(
                lambda col_name: F.col("`{0}`".format(col_name)).alias(fun(col_name)),
                df.columns,
            )
        )
        return df.select(*cols)
    return _

def convert_snake_to_camel(key: str) -> str:
    """
    Coerce snake case to camel case

	Use split with destructuring to grab first word and store rest in list.
    map over the rest of the words passing them to str.capitalize(), 
    destructure map list elements into new list with first word at front, and
    finally join them back to a str.
    
    Note: may want to use `str.title` instead of `str.capitalize`
    Method extracted from https://www.codegrepper.com/code-examples/python/python+convert+snake+case+to+camel+case

    :param key: the word or variable to convert
    :return str: a camel cased version of the key
    """

    first, *rest = key.split('_')

    camel_key: list = [first.lower(), *map(str.capitalize, rest)]

    return ''.join(camel_key)

def write_subset_to_parquet(df: DataFrame, bucket: str, dataset: str, subset_size: int) -> None:
    """
    > Write a subset of the dataframe to a parquet file in the specified bucket
    
    Args:
      df (DataFrame): The DataFrame to write to parquet
      bucket (str): the name of the bucket where the data is stored
      dataset (str): The name of the dataset to be used.
      subset_size (int): the number of rows to write to the subset
    """
    output_path = f"{bucket}/{dataset}"
    df.limit(subset_size).coalesce(1).write.parquet(output_path)