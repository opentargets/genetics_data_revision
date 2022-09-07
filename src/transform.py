import hydra
from omegaconf import DictConfig
from pyspark.sql import SparkSession

from src.utils import *

DATASET_TRANSFORMATIONS = {
    'v2d_coloc': transform_v2d_coloc,
    'd2v2g': transform_d2v2g,
    'd2v2g_scored': transform_d2v2g,
    'l2g': transform_l2g,
    'manhattan': transform_manhattan,
    'lut/overlap-index': transform_study_overlap,
    'v2d': transform_v2d,
    'v2d_credset': transform_v2d_credset,
    'sa/gwas': transform_sa_gwas,
    'sa/molecular_trait': transform_sa_molecular_trait,
    'v2g': transform_v2g,
    'v2g_scored': transform_v2g,
    'variant-index': transform_variant_index,
    'lut/variant-index': transform_variant_index,
}


@hydra.main(config_path=get_cwd(), config_name="config")
def main(cfg: DictConfig) -> None:

    spark = SparkSession.builder.getOrCreate()

    all_data_paths = get_datasets_paths(cfg.data_repositories.release_output_root, cfg.datasets)

    for (data_name, data_path) in all_data_paths:
        data = spark.read.parquet(data_path)
        # Apply dataset-specific transformations
        data = data.transform(DATASET_TRANSFORMATIONS['data_name'])
        # Apply global transformations
        data = camel_case_col_names(data)
        # Write to parquet
        write_subset_to_parquet(data, cfg.data_repositories.transformed_data_root, data_name, cfg.subset_size)

    spark.stop()


if __name__ == '__main__':
    main()
