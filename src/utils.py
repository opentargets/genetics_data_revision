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


def transform_v2d_coloc(v2d_coloc: DataFrame) -> DataFrame:
    return (
        v2d_coloc.withColumn(
            'left_variant_id',
            F.concat_ws('_', F.col('left_chrom'), F.col('left_pos'), F.col('left_ref'), F.col('left_alt')),
        )
        .withColumn(
            'right_variant_id',
            F.concat_ws('_', F.col('right_chrom'), F.col('right_pos'), F.col('right_ref'), F.col('right_alt')),
        )
        .drop(
            'left_chrom',
            'left_pos',
            'left_ref',
            'left_alt',
            'right_chrom',
            'right_pos',
            'right_ref',
            'right_alt',
            'left_type',
            'right_type',
            'right_gene_id',
            'right_bio_feature',
        )
    )


def transform_d2v2g(d2v2g: DataFrame) -> DataFrame:
    return (
        d2v2g.withColumn(
            'lead_variant_id',
            F.concat_ws('_', F.col('lead_chrom'), F.col('lead_pos'), F.col('lead_ref'), F.col('lead_alt')),
        )
        .withColumn(
            'tag_variant_id', F.concat_ws('_', F.col('tag_chrom'), F.col('tag_pos'), F.col('tag_ref'), F.col('tag_alt'))
        )
        .drop(
            'pub_date',
            'pub_journal',
            'pub_title',
            'pub_author',
            'has_sumstats',
            'trait_reported',
            'trait_efos',
            'ancestry_initial',
            'ancestry_replication',
            'n_initial',
            'n_replication',
            'n_cases',
            'num_assoc_loci',
            'lead_chrom',
            'lead_pos',
            'lead_ref',
            'lead_alt',
            'pmid',
            'source',
            'tag_chrom',
            'tag_pos',
            'tag_ref',
            'tag_alt',
            'AFR_1000G_prop',
            'AMR_1000G_prop',
            'EAS_1000G_prop',
            'EUR_1000G_prop',
            'SAS_1000G_prop',
        )
    )


def transform_l2g(l2g: DataFrame) -> DataFrame:
    return l2g.withColumn(
        'variant_id', F.concat_ws('_', F.col('chrom'), F.col('pos'), F.col('ref'), F.col('alt'))
    ).drop('chrom', 'pos', 'ref', 'alt')


def transform_manhattan(manhattan: DataFrame) -> DataFrame:
    return manhattan.withColumn(
        'variant_id', F.concat_ws('_', F.col('chrom'), F.col('pos'), F.col('ref'), F.col('alt'))
    ).drop('chrom', 'pos', 'ref', 'alt')


def transform_study_overlap(study_overlap: DataFrame) -> DataFrame:
    return (
        study_overlap.withColumn(
            'A_variant_id', F.concat_ws('_', F.col('A_chrom'), F.col('A_pos'), F.col('A_ref'), F.col('A_alt'))
        )
        .withColumn('B_variant_id', F.concat_ws('_', F.col('B_chrom'), F.col('B_pos'), F.col('B_ref'), F.col('B_alt')))
        .drop('A_chrom', 'A_pos', 'A_ref', 'A_alt', 'B_chrom', 'B_pos', 'B_ref', 'B_alt')
    )


def transform_v2d(v2d: DataFrame) -> DataFrame:
    return (
        v2d.withColumn(
            'lead_variant_id',
            F.concat_ws('_', F.col('lead_chrom'), F.col('lead_pos'), F.col('lead_ref'), F.col('lead_alt')),
        )
        .withColumn(
            'tag_variant_id', F.concat_ws('_', F.col('tag_chrom'), F.col('tag_pos'), F.col('tag_ref'), F.col('tag_alt'))
        )
        .drop(
            'pmid',
            'pub_date',
            'pub_journal',
            'pub_title',
            'pub_author',
            'has_sumstats',
            'trait_reported',
            'trait_efos',
            'ancestry_initial',
            'ancestry_replication',
            'n_initial',
            'n_replication',
            'n_cases',
            'trait_category',
            'num_assoc_loci',
            'lead_chrom',
            'lead_pos',
            'lead_ref',
            'lead_alt',
            'tag_chrom',
            'tag_pos',
            'tag_ref',
            'tag_alt',
            'source',
            'AFR_1000G_prop',
            'AMR_1000G_prop',
            'EAS_1000G_prop',
            'EUR_1000G_prop',
            'SAS_1000G_prop',
        )
    )


def transform_v2d_credset(v2d_credset: DataFrame) -> DataFrame:
    return (
        v2d_credset.withColumn('lead_variant_id', F.translate('lead_variant_id', '\:', '\_'))
        .withColumn('tag_variant_id', F.translate('lead_variant_id', '\:', '\_'))
        .drop(
            'lead_chrom',
            'lead_pos',
            'lead_ref',
            'lead_alt',
            'tag_chrom',
            'tag_pos',
            'tag_ref',
            'tag_alt',
            'bio_feature',
            'phenotype_id',
            'gene_id',
        )
    )


def transform_sa_gwas(sa_gwas: DataFrame) -> DataFrame:
    return sa_gwas.withColumn(
        'variant_id', F.concat_ws('_', F.col('chrom'), F.col('pos'), F.col('ref'), F.col('alt'))
    ).drop('chrom', 'pos', 'ref', 'alt', 'type_id', 'n_total', 'n_cases')


def transform_sa_molecular_trait(sa_molecular_trait: DataFrame) -> DataFrame:
    return sa_molecular_trait.withColumn(
        'variant_id', F.concat_ws('_', F.col('chrom'), F.col('pos'), F.col('ref'), F.col('alt'))
    ).drop('chrom', 'pos', 'ref', 'alt', 'type_id', 'phenotype_id', 'gene_id', 'bio_feature')


def transform_v2g(v2g: DataFrame) -> DataFrame:
    return v2g.withColumn(
        'variant_id', F.concat_ws('_', F.col('chr_id'), F.col('position'), F.col('ref_allele'), F.col('alt_allele'))
    ).drop('chr_id', 'position', 'ref_allele', 'alt_allele', 'gene_id')


def transform_variant_index(variant: DataFrame) -> DataFrame:
    return variant.withColumn(
        'variant_id', F.concat_ws('_', F.col('chr_id'), F.col('position'), F.col('ref_allele'), F.col('alt_allele'))
    ).drop('chrom', 'pos', 'ref', 'alt')
